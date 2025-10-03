# newOrderValidation.py
import os
import time
from dotenv import load_dotenv
import time  # (kept as in your original)
from pathlib import Path
from app.env import load_environment
import requests
import logging
import pandas as pd
from .sqlQueries import *
import json
import uuid
import traceback
from datetime import datetime
import threading

# =======================
# TELEMETRY TOGGLE & FILE LOGGING
# =======================
# Flip this to False to disable detailed logs (or set env ITEMPATH_COLLECT_TELEMETRY=false)
COLLECT_TELEMETRY = os.getenv("ITEMPATH_COLLECT_TELEMETRY", "true").lower() in ("1", "true", "yes", "on")

# File logging toggles
LOG_TO_FILE = os.getenv("ITEMPATH_LOG_TO_FILE", "true").lower() in ("1", "true", "yes", "on")
ITEMPATH_LOG_DIR = os.getenv("ITEMPATH_LOG_DIR", "./logs")
# If you pass the same RUN_ID for your whole process invocation, all logs land in one file
RUN_ID = os.getenv("ITEMPATH_RUN_ID", f"run-{int(time.time())}-{uuid.uuid4().hex[:6]}")
# If True, each thread gets its own file (suffix with thread id)
ITEMPATH_PER_THREAD_LOGS = os.getenv("ITEMPATH_PER_THREAD_LOGS", "false").lower() in ("1", "true", "yes", "on")

# How much to log (when enabled)
TELEM_PREVIEW_LINES = int(os.getenv("ITEMPATH_TELEM_PREVIEW_LINES", "3"))
TELEM_MAX_COLS = int(os.getenv("ITEMPATH_TELEM_MAX_COLS", "20"))
TELEM_HTTP_TIMEOUT = int(os.getenv("ITEMPATH_HTTP_TIMEOUT", "300"))  # keeps your original default

# --- New: creation/visibility grace windows (keep tiny to preserve speed) ---
# If order POST times out (504), we give a small grace window to see if it landed anyway.
CREATE_GRACE_SEC = float(os.getenv("ITEMPATH_CREATE_GRACE_SEC", "6"))
CREATE_POLL_INTERVAL = float(os.getenv("ITEMPATH_CREATE_POLL_INTERVAL", "0.75"))

# Before add-to-batch, if the order or WO lines are not yet visible, do a brief pre-wait.
ADD_PREWAIT_SEC = float(os.getenv("ITEMPATH_ADD_PREWAIT_SEC", "4"))
ADD_PREWAIT_INTERVAL = float(os.getenv("ITEMPATH_ADD_PREWAIT_INTERVAL", "0.5"))
WOL_POLL_ATTEMPTS = int(os.getenv("ITEMPATH_WOL_POLL_ATTEMPTS", "3"))
WOL_POLL_INTERVAL = float(os.getenv("ITEMPATH_WOL_POLL_INTERVAL", "0.5"))

# Sentinel for "defer creation until final tail" (avoids dupes)
DEFER_CREATE_SENTINEL = {"defer_create": True}

env_config = load_environment()

base_url = (env_config.get('ITEMPATH_URL') or '').rstrip('/')
auth_token = env_config.get('ITEMPATH_APPLICATION_TOKEN')

# Configure console logging if nothing is configured
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =======================
# File logger init (single run file or per-thread)
# =======================
_file_handlers_by_path = {}
_current_log_file_path = None

def _ensure_log_dir(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)

def _compute_log_path(run_id: str, per_thread: bool) -> str:
    date = datetime.utcnow().strftime("%Y%m%d")
    if per_thread:
        tid = threading.get_ident()
        return os.path.join(ITEMPATH_LOG_DIR, f"itempath_{date}_{run_id}_T{tid}.log")
    return os.path.join(ITEMPATH_LOG_DIR, f"itempath_{date}_{run_id}.log")

def _attach_file_handler_if_needed():
    global _current_log_file_path
    if not LOG_TO_FILE:
        return
    _ensure_log_dir(ITEMPATH_LOG_DIR)
    path = _compute_log_path(RUN_ID, ITEMPATH_PER_THREAD_LOGS)
    if path in _file_handlers_by_path:
        _current_log_file_path = path
        return
    fh = logging.FileHandler(path, encoding="utf-8")
    # Our log_event already emits JSON; formatter should not add prefixes
    fh.setFormatter(logging.Formatter("%(message)s"))
    fh.setLevel(logging.INFO)
    root = logging.getLogger()  # root catches logging.info(...) used in log_event
    root.addHandler(fh)
    _file_handlers_by_path[path] = fh
    _current_log_file_path = path
    # Emit a marker line so you can find the file start
    try:
        logging.info(json.dumps({
            "ts": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
            "event": "telemetry.file.start",
            "run_id": RUN_ID,
            "path": path
        }))
    except Exception:
        pass

def get_current_log_file() -> str | None:
    """Return the path of the current log file (for this thread if per-thread)."""
    return _current_log_file_path

# Attach a file handler immediately on import (safe/no-op if disabled)
_attach_file_handler_if_needed()

# =======================
# Telemetry helpers
# =======================
_REDACT_HEADERS = {"authorization", "proxy-authorization"}

def _now_iso():
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

def _rid(key: str):
    return f"{key}-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}"

def _redact_headers(h: dict | None):
    if not h:
        return {}
    out = {}
    for k, v in h.items():
        out[k] = "***" if k.lower() in _REDACT_HEADERS else v
    return out

def _short(s: str | None, n: int = 240):
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n] + f"...(+{len(s)-n})"

def _df_profile(df: pd.DataFrame, key_cols: list[str] | None = None):
    if not COLLECT_TELEMETRY:
        return None
    try:
        if df is None:
            return {"none": True}
        if df.empty:
            return {"empty": True, "columns": list(df.columns)}
        cols = list(df.columns)[:TELEM_MAX_COLS]
        dtypes = {c: str(df.dtypes[c]) for c in cols}
        head = df.iloc[0].to_dict()
        pick = {k: head.get(k) for k in (key_cols or []) if k in head}
        return {
            "shape": list(df.shape),
            "columns": cols,
            "dtypes": dtypes,
            "head_keys": pick
        }
    except Exception as e:
        return {"error": f"df_profile:{type(e).__name__}:{_short(str(e))}"}

def _preview_lines(order_lines: list, limit: int = TELEM_PREVIEW_LINES) -> list:
    if not COLLECT_TELEMETRY:
        return []
    if not order_lines:
        return []
    pv = []
    for ln in order_lines[:limit]:
        try:
            pv.append({
                "materialId": ln.get("materialId"),
                "qty": ln.get("quantity"),
                "qual": ln.get("qualification"),
                "hasSU": bool("storageUnits" in ln),
                "info1_len": len(ln.get("Info1") or ""),
                "info2_len": len(ln.get("Info2") or "")
            })
        except Exception:
            pv.append({"_raw": str(ln)[:180]})
    return pv

def log_event(event: str, **fields):
    if not COLLECT_TELEMETRY:
        return
    # Ensure file handler for new threads if per-thread files enabled
    _attach_file_handler_if_needed()
    try:
        payload = {"ts": _now_iso(), "event": event, "run_id": RUN_ID, **fields}
        logging.info(json.dumps(payload, ensure_ascii=False))
    except Exception:
        # never let logging break the app
        logging.info(f'{{"ts":"{_now_iso()}","event":"telemetry.error","run_id":"{RUN_ID}","note":"failed to serialize log"}}')

def _http_call(method: str, url: str, *, headers=None, json_body=None, timeout=TELEM_HTTP_TIMEOUT, rid=None):
    start = time.perf_counter()
    hdrs = dict(headers or {})
    if rid:
        hdrs["X-Request-Id"] = rid

    if COLLECT_TELEMETRY:
        log_event("http.send",
                  rid=rid, method=method, url=url,
                  headers=_redact_headers(hdrs),
                  body_preview=_preview_lines(json_body.get("order_lines", []), TELEM_PREVIEW_LINES) if isinstance(json_body, dict) and "order_lines" in json_body else None,
                  body_keys=list(json_body.keys()) if isinstance(json_body, dict) else None)

    try:
        resp = requests.request(method=method, url=url, headers=hdrs, json=json_body, timeout=timeout)
        ms = round((time.perf_counter() - start) * 1000, 1)
        if COLLECT_TELEMETRY:
            try:
                body = resp.json()
                shape = {"_type": "json", "keys": list(body.keys())[:10]}
            except Exception:
                body = None
                txt_len = len(getattr(resp, "text", "") or "")
                shape = {"_type": "text", "len": txt_len}
            log_event("http.ack",
                      rid=rid, status=resp.status_code, ms=ms, url=url,
                      resp_ct=resp.headers.get("Content-Type"),
                      resp_cl=resp.headers.get("Content-Length"),
                      body_shape=shape)
        return resp
    except Exception as e:
        ms = round((time.perf_counter() - start) * 1000, 1)
        log_event("http.error", rid=rid, url=url, ms=ms,
                  error_type=type(e).__name__, error=_short(str(e)),
                  tb=_short(traceback.format_exc(), 800))
        raise

def _require_env():
    if not base_url or not auth_token:
        raise ValueError("ITEMPATH_URL and ITEMPATH_APPLICATION_TOKEN must be set in the .env file")

# =======================
# Visibility helpers (new)
# =======================

def _wait_for_order_visible_in_ppg(orderName: str, timeout_sec: float, poll_interval: float) -> bool:
    """
    Polls PPG briefly to see if the order becomes visible after a slow/async create.
    Returns True if found, False otherwise.
    """
    t0 = time.perf_counter()
    tries = 0
    while (time.perf_counter() - t0) < timeout_sec:
        tries += 1
        dfPPG = run_getOpenPPGPickOrders_specific(orderName=orderName)
        log_event("order.visible.check", order=orderName,
                  found=not dfPPG.empty, tries=tries,
                  profile=_df_profile(dfPPG, ["MasterorderId", "OrderstatusType"]))
        if not dfPPG.empty:
            return True
        time.sleep(poll_interval)
    return False

def _wait_for_wolines(order_id: str, max_attempts: int, poll_interval: float) -> pd.DataFrame:
    """
    Quickly polls for WO lines to appear. Returns the last dataframe (possibly empty).
    """
    df = run_getOpenPPGWOLines_specific(order_id=order_id)
    if not df.empty:
        return df
    for _ in range(max_attempts - 1):
        time.sleep(poll_interval)
        df = run_getOpenPPGWOLines_specific(order_id=order_id)
        if not df.empty:
            break
    return df

# =======================
# Your functions (instrumented, unchanged behavior with robustness)
# =======================

def checkAllocation(dfPPG_order):
    if not dfPPG_order.empty:  # Fixed: was checking if empty
        return dfPPG_order.iloc[0]['OrderstatusType'] in [10, 11]
    return False

# create_pick_order

def deleteOrder_itemPathHelper(order_id):
    """
    Deletes an order from the API with specified ID.
    Handles 404s and 504s gracefully.

    Returns:
        dict: JSON response on success (unchanged) OR underlying Response/None on error (unchanged pattern)
    """
    _require_env()
    url = f"{base_url}/api/orders/{order_id}/delete"
    rid = _rid(f"del:{order_id}")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    log_event("order.delete.send", rid=rid, orderId=order_id, url=url)
    try:
        response = _http_call("DELETE", url, headers=headers, timeout=TELEM_HTTP_TIMEOUT, rid=rid)
        response.raise_for_status()
        out = {}
        try:
            out = response.json()
        except Exception:
            pass
        log_event("order.delete.ok", rid=rid, orderId=order_id, status=response.status_code, resp_keys=list(out.keys())[:10])
        return out
    except Exception as e:
        log_event("order.delete.fail", rid=rid, orderId=order_id, error_type=type(e).__name__, error=_short(str(e)))
        # preserve original "except: return response" (but avoid NameError if response undefined)
        return locals().get("response", None)

# delete order
def deleteOrder(orderName):
    start = time.perf_counter()
    deleteCount = 0
    while deleteCount <= 60:
        dfPPG = run_getOpenPPGPickOrders_specific(orderName=orderName)
        log_event("ppg.orders.snapshot",
                  order=orderName,
                  profile=_df_profile(dfPPG, ["MasterorderId", "OrderstatusType"]),
                  loop=deleteCount)

        if dfPPG.empty:
            elapsed = round((time.perf_counter() - start) * 1000, 1)
            logging.info(f"Order {orderName} no longer exists!")
            log_event("order.delete.done", order=orderName, tries=deleteCount, elapsed_ms=elapsed)
            return True

        order_id = dfPPG.iloc[0]['MasterorderId']
        deleted = deleteOrder_itemPathHelper(order_id=order_id)
        logging.info(f"Attempted to delete: {orderName} - {deleted}")
        time.sleep(1)
        deleteCount += 1

    elapsed = round((time.perf_counter() - start) * 1000, 1)
    log_event("order.delete.timeout", order=orderName, tries=deleteCount, elapsed_ms=elapsed)
    return False

def deAllocateOrder_itemPathHelper(order_id):
    """
    Allocates or deallocates the order with the given ID.
    Returns Response on success; None on failure (original pattern).
    """
    _require_env()
    url = f"{base_url}/api/orders/{order_id}"
    rid = _rid(f"dealloc:{order_id}")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    allocateOrDeallocate = False

    data = {
        "allocate": True if allocateOrDeallocate else False,
        "deallocate": True if not allocateOrDeallocate else False
    }

    log_event("order.dealloc.send", rid=rid, orderId=order_id, url=url, body=data)
    try:
        response = _http_call("PUT", url, headers=headers, json_body=data, timeout=TELEM_HTTP_TIMEOUT, rid=rid)
        response.raise_for_status()
        log_event("order.dealloc.ok", rid=rid, orderId=order_id, status=response.status_code)
        return response
    except Exception as e:
        log_event("order.dealloc.fail", rid=rid, orderId=order_id, error_type=type(e).__name__, error=_short(str(e)))
        return None

# de-allocate order
def deAllocateOrder(orderName):
    start = time.perf_counter()
    refreshCount = 0
    while refreshCount <= 60:
        dfPPG = run_getOpenPPGPickOrders_specific(orderName=orderName)
        log_event("ppg.orders.snapshot",
                  order=orderName,
                  profile=_df_profile(dfPPG, ["MasterorderId", "OrderstatusType"]),
                  loop=refreshCount)

        if dfPPG.empty:
            log_event("order.dealloc.done", order=orderName, reason="order_missing",
                      elapsed_ms=round((time.perf_counter() - start) * 1000, 1))
            return True
        if not checkAllocation(dfPPG_order=dfPPG):
            log_event("order.dealloc.done", order=orderName, reason="not_allocated",
                      elapsed_ms=round((time.perf_counter() - start) * 1000, 1))
            return True

        order_id = dfPPG.iloc[0]['MasterorderId']
        deAllocated = deAllocateOrder_itemPathHelper(order_id=order_id)
        logging.info(f"Attempting to deallocate {orderName} - {deAllocated}")
        time.sleep(1)
        refreshCount += 1

    log_event("order.dealloc.timeout", order=orderName, tries=refreshCount,
              elapsed_ms=round((time.perf_counter() - start) * 1000, 1))
    return False

def createFullSOPickOrder(orderNameStr, deadline, materials_array, pick_qty_array, orderLineArr):
    """
    Creates a full SO pick order - NO RETRIES, designed to timeout quickly
    (keeps your original timeout of 300 unless overridden by ITEMPATH_HTTP_TIMEOUT)

    Returns:
      - dict (server JSON) or True on success/exists
      - DEFER_CREATE_SENTINEL ({"defer_create": True}) on 500 errors (to retry at final tail)
      - True on 504/timeout paths where we allow add-to-batch deferral
      - False on definitive failures (e.g., other 4xx/5xx we don't want to retry)
    """
    _require_env()
    endpoint = f"{base_url}/api/orders"
    rid = _rid(orderNameStr)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    # Create order lines
    t0_build = time.perf_counter()
    order_lines = []
    for material, quantity, orderLine in zip(materials_array, pick_qty_array, orderLineArr):
        order_line = {
            "materialId": material['id'] if isinstance(material, dict) else material,  # Fixed: handle both dict and string
            "quantity": quantity,
            "Info1": orderLine['Info1'] if orderLine['Info1'] is not None else "",
            "Info2": orderLine['Info2'] if orderLine['Info2'] is not None else "",
            "qualification": orderLine['qualification']
        }
        # Add storage units based on GroupCode
        if isinstance(material, dict) and 'GroupCode' in material:
            if material['GroupCode'] == '121':
                order_line["storageUnits"] = [{"name": "G1"},{"name": "G2"},{"name": "G3"},{"name": "G4"}]
            elif material['GroupCode'] == '112':
                order_line["storageUnits"] = [{"name": "V1"},{"name": "V2"},{"name": "V3"},{"name": "V4"},{"name": "V5"}]
        order_lines.append(order_line)

    build_ms = round((time.perf_counter() - t0_build) * 1000, 1)

    request_body = {
        "name": orderNameStr,
        "directionType": 2,
        "allocate": True,
        "deadline": deadline,
        "order_lines": order_lines
    }

    log_event("order.create.send",
              rid=rid, order=orderNameStr, deadline=deadline,
              n_lines=len(order_lines), build_ms=build_ms,
              preview=_preview_lines(order_lines))

    try:
        response = _http_call(
            "POST",
            endpoint,
            headers=headers,
            json_body=request_body,
            timeout=TELEM_HTTP_TIMEOUT,  # defaults to 300 to match your original
            rid=rid
        )
        # Some APIs return 201 or 200 on create
        response.raise_for_status()
        data = {}
        try:
            data = response.json()
        except Exception:
            pass
        log_event("order.create.ok", rid=rid, order=orderNameStr, status=response.status_code,
                  resp_keys=list(data.keys())[:12])
        return data

    except requests.exceptions.HTTPError as e:
        # Special handling for 409 (already exists), 504 (GW timeout), and 500 (server error)
        status = getattr(e.response, "status_code", None)
        if status == 409:
            log_event("order.create.exists", rid=rid, order=orderNameStr, status=status)
            return True  # treat as created
        if status == 504:
            # Gateway timeout: often accepted and processed late.
            logger.error(f"Create order 504 for {orderNameStr}; checking for eventual visibility...")
            log_event("order.create.timeout", rid=rid, order=orderNameStr, status=status, note="gateway_timeout_check_visibility")
            appeared = _wait_for_order_visible_in_ppg(orderNameStr, CREATE_GRACE_SEC, CREATE_POLL_INTERVAL)
            if appeared:
                log_event("order.create.late_visible", rid=rid, order=orderNameStr, within_sec=CREATE_GRACE_SEC)
                return True
            log_event("order.create.deferred", rid=rid, order=orderNameStr, reason="not_visible_within_grace")
            return True  # allow upstream deferral via add-to-batch failure
        if status == 500:
            # Do NOT retry now; mark for final create attempt to avoid duplicates.
            logger.error(f"Create order 500 (defer) for {orderNameStr}")
            log_event("order.create.server_error.defer", rid=rid, order=orderNameStr, status=status)
            return DEFER_CREATE_SENTINEL

        # Other HTTP errors: log and return False (caller records as create_failed)
        logger.error(f"Error creating order: {str(e)}")
        log_event("order.create.fail", rid=rid, order=orderNameStr,
                  error_type=type(e).__name__, error=_short(str(e)), tb=_short(traceback.format_exc(), 800))
        return False

    except Exception as e:
        # Network timeouts or other exceptions — same strategy as 504: check visibility briefly
        logger.error(f"Create order error (non-HTTP): {str(e)}")
        log_event("order.create.error", rid=rid, order=orderNameStr,
                  error_type=type(e).__name__, error=_short(str(e)), tb=_short(traceback.format_exc(), 800))
        appeared = _wait_for_order_visible_in_ppg(orderNameStr, CREATE_GRACE_SEC, CREATE_POLL_INTERVAL)
        if appeared:
            log_event("order.create.late_visible", rid=rid, order=orderNameStr, within_sec=CREATE_GRACE_SEC)
            return True
        log_event("order.create.deferred", rid=rid, order=orderNameStr, reason="non_http_error_not_visible")
        # Return True to allow upstream to treat it like a WO-line deferral path.
        return True

def createOrder(orderName, df_sapBOM):
    t_start = time.perf_counter()

    # Pre-check: already present?
    dfPPG = run_getOpenPPGPickOrders_specific(orderName=orderName)
    log_event("ppg.orders.snapshot.preCreate",
              order=orderName,
              profile=_df_profile(dfPPG, ["MasterorderId", "OrderstatusType"]))
    if not dfPPG.empty:
        logging.info(f"CAUGHT ERROR: {orderName} trying to be created but already exists.")
        log_event("order.create.skipped", order=orderName, reason="already_exists_in_ppg",
                  elapsed_ms=round((time.perf_counter() - t_start) * 1000, 1))
        return True  # treat as created

    df_sapBOMFiltered = df_sapBOM[df_sapBOM['U_PLS_PPG_ITEM'] == 'Y']
    log_event("sap.bom.filter", order=orderName,
              in_shape=list(df_sapBOM.shape) if isinstance(df_sapBOM, pd.DataFrame) else None,
              out_shape=list(df_sapBOMFiltered.shape) if isinstance(df_sapBOMFiltered, pd.DataFrame) else None)

    if df_sapBOMFiltered.empty:
        log_event("order.create.skipped", order=orderName, reason="no_bom_lines",
                  elapsed_ms=round((time.perf_counter() - t_start) * 1000, 1))
        return False  # failed to create as no BOM

    df_sap = df_sapBOMFiltered.sort_values(by='LineNum', ascending=True).reset_index(drop=True)
    df_sap['number'] = df_sap.index + 1

    # Rename columns
    df_sap = df_sap.rename(columns={
        'FreeTxt': 'Info1',
        'Dscription': 'Info2',
        'WhsCode': 'qualification',
        'ItemCode': 'materialName'
    })

    # Extract deadline
    deadline_sap = df_sap['DocDueDate'].iloc[0]
    if isinstance(deadline_sap, str):
        deadline_sap = deadline_sap.replace(' ', 'T')
    else:
        from datetime import datetime as _dt
        deadline_sap = deadline_sap.strftime('%Y-%m-%dT%H:%M:%S')

    # Prepare materials array
    t0_prep = time.perf_counter()
    materialArr_sap = []
    pickQtyArr_sap = []
    orderLineArr_sap = []

    for index, row in df_sap.iterrows():
        # Fixed: Create material dict with id and GroupCode
        material_dict = {
            'id': row['MaterialId'],
            'GroupCode': str(row['ItmsGrpCod']) if pd.notna(row['ItmsGrpCod']) else None
        }
        materialArr_sap.append(material_dict)
        pickQtyArr_sap.append(float(row['Quantity']))

        orderLineArr_sap.append({
            "Info1": row["Info1"] if pd.notna(row["Info1"]) else "",
            "Info2": row["Info2"] if pd.notna(row["Info2"]) else "",
            "materialName": row["materialName"],
            "qualification": row["qualification"]
        })
    prep_ms = round((time.perf_counter() - t0_prep) * 1000, 1)

    log_event("order.create.prepare",
              order=orderName,
              n_lines=len(orderLineArr_sap),
              prep_ms=prep_ms,
              first_line={k: orderLineArr_sap[0].get(k) for k in ["materialName","qualification","Info1","Info2"]} if orderLineArr_sap else None)

    created = createFullSOPickOrder(
            orderName,
            deadline_sap,
            materialArr_sap,
            pickQtyArr_sap,
            orderLineArr_sap
        )

    logging.info(f"Created order {orderName} - {created}")

    # Post-check snapshot (even if created=True from a timeout path, this helps telemetry)
    dfPPG_after = run_getOpenPPGPickOrders_specific(orderName=orderName)
    log_event("ppg.orders.snapshot.postCreate",
              order=orderName,
              profile=_df_profile(dfPPG_after, ["MasterorderId", "OrderstatusType"]),
              elapsed_ms=round((time.perf_counter() - t_start) * 1000, 1))

    # created can be dict(True), True, False, or DEFER_CREATE_SENTINEL
    return created

def update_work_order_line(work_order_line_id, batch_id, handling_rank=1):
    """
    Updates a work order line with batch and handling rank information.
    Returns response.json() on success; Response/None on failure (preserves your pattern).
    """
    _require_env()
    url = f"{base_url.rstrip('/')}/api/work_order_lines/{work_order_line_id}"
    rid = _rid(f"wol:{work_order_line_id}")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    data = {
        "batchId": batch_id,
        "handlingRank": handling_rank
    }

    log_event("wol.assign.send", rid=rid, wol=work_order_line_id, batchId=batch_id, rank=handling_rank, url=url)
    try:
        response = _http_call("PUT", url, headers=headers, json_body=data, timeout=TELEM_HTTP_TIMEOUT, rid=rid)
        response.raise_for_status()
        log_event("wol.assign.ok", rid=rid, wol=work_order_line_id, status=response.status_code)
        try:
            return response.json()
        except Exception:
            return response
    except Exception as e:
        log_event("wol.assign.fail", rid=rid, wol=work_order_line_id,
                  error_type=type(e).__name__, error=_short(str(e)))
        return locals().get("response", None)

def createBatch_itemPathHelper(batchName):
    """
    Creates a new batch with the given name.
    Returns response.json() on success; Response/None on failure (keeps your semantics).
    """
    _require_env()
    url = f"{base_url}/api/batches"
    rid = _rid(f"batch:{batchName}")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    data = {
        "name": batchName,
        "type": 2
    }

    log_event("batch.create.send", rid=rid, batch=batchName, url=url)
    try:
        response = _http_call("POST", url, headers=headers, json_body=data, timeout=TELEM_HTTP_TIMEOUT, rid=rid)
        response.raise_for_status()
        resp_json = {}
        try:
            resp_json = response.json()
        except Exception:
            pass
        log_event("batch.create.ok", rid=rid, batch=batchName, status=response.status_code, resp_keys=list(resp_json.keys())[:10])
        return resp_json
    except Exception as e:
        log_event("batch.create.fail", rid=rid, batch=batchName, error_type=type(e).__name__, error=_short(str(e)))
        return locals().get("response", None)

def createBatch(batchName):

    logging.info(f"Trying to create batch {batchName}")

    count = 0
    while count <= 60:
        dfPPG_batch = run_getOpenPPGBatches_specific(batchName=batchName)
        log_event("ppg.batch.snapshot", batch=batchName, loop=count, profile=_df_profile(dfPPG_batch, ["WorkorderId"]))
        if not dfPPG_batch.empty:
            logging.info(f"Batch {batchName} has been created.")
            return True

        createBatch_itemPathHelper(batchName=batchName)
        time.sleep(7)
        count +=1
    log_event("batch.create.timeout", batch=batchName, tries=count)
    return False

def update_work_order_line_itempathhelper(work_order_line_id, batch_id, handling_rank=1):
    """
    Updates a work order line with batch and handling rank information.
    Returns Response on success; None on failure (keeps your calling pattern in addOrderToBatch).
    """
    _require_env()
    url = f"{base_url.rstrip('/')}/api/work_order_lines/{work_order_line_id}"
    rid = _rid(f"wol:{work_order_line_id}")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }

    data = {
        "batchId": batch_id,
        "handlingRank": handling_rank
    }

    log_event("wol.assign.send", rid=rid, wol=work_order_line_id, batchId=batch_id, rank=handling_rank, url=url)
    try:
        response = _http_call("PUT", url, headers=headers, json_body=data, timeout=TELEM_HTTP_TIMEOUT, rid=rid)
        log_event("wol.assign.ack", rid=rid, wol=work_order_line_id, status=response.status_code)
        return response
    except Exception as e:
        log_event("wol.assign.fail", rid=rid, wol=work_order_line_id, error_type=type(e).__name__, error=_short(str(e)))
        return None

def addOrderToBatch(orderName, batchName, orderRank):
    # Pre-wait briefly for the order to appear if missing (handles slow creates)
    dfPPG_order = run_getOpenPPGPickOrders_specific(orderName=orderName)
    if dfPPG_order.empty and ADD_PREWAIT_SEC > 0:
        log_event("batch.assign.prewait.order_missing", order=orderName, batch=batchName, wait_sec=ADD_PREWAIT_SEC)
        t0 = time.perf_counter()
        while (time.perf_counter() - t0) < ADD_PREWAIT_SEC and dfPPG_order.empty:
            time.sleep(ADD_PREWAIT_INTERVAL)
            dfPPG_order = run_getOpenPPGPickOrders_specific(orderName=orderName)

    log_event("ppg.orders.snapshot", order=orderName, stage="addOrderToBatch.pre", profile=_df_profile(dfPPG_order, ["MasterorderId", "OrderstatusType"]))
    if dfPPG_order.empty:
        logging.info(f"Tried to add {orderName} to {batchName}. But Order does not exist.")
        log_event("batch.assign.skipped", order=orderName, batch=batchName, reason="order_missing_in_ppg")
        return False

    logging.info(f"DF PPG Order: {dfPPG_order}")

    # getWorkOrderLines (with a short visibility poll)
    order_id = dfPPG_order.iloc[0]['MasterorderId']
    dfPPG_WOLines = _wait_for_wolines(order_id=str(order_id), max_attempts=WOL_POLL_ATTEMPTS, poll_interval=WOL_POLL_INTERVAL)
    log_event("ppg.wolines.snapshot", order=orderName, stage="addOrderToBatch.pre", profile=_df_profile(dfPPG_WOLines, ["WorkorderlineId","MasterorderId"]))
    if dfPPG_WOLines.empty:
        logging.info(f"Tried to get WO Lines for {orderName} but none exist.")
        log_event("batch.assign.skipped", order=orderName, batch=batchName, reason="wo_lines_missing_in_ppg")
        return False

    logging.info(f"DF WO Lines: {dfPPG_WOLines}")
    # create the batch we are going to use
    if not createBatch(batchName=batchName):
        logging.info(f"Batch {batchName} could not be created!")
        log_event("batch.assign.fail", order=orderName, batch=batchName, stage="create_batch")
        return False

    logging.info(f"Trying to get batch")
    dfPPG_batch = run_getOpenPPGBatches_specific(batchName=batchName)
    log_event("ppg.batch.snapshot", batch=batchName, stage="addOrderToBatch.fetch_batch", profile=_df_profile(dfPPG_batch, ["WorkorderId"]))
    if dfPPG_batch.empty:
        logging.info(f"Batch {batchName} was created but dissapeared right after :(")
        log_event("batch.assign.fail", order=orderName, batch=batchName, stage="fetch_batch")
        return False

    logging.info(f"DF Batch: {dfPPG_batch}")
    batch_id = dfPPG_batch.iloc[0]['WorkorderId']

    logging.info(f"Adding WO lines")
    log_event("batch.assign.begin", order=orderName, batch=batchName, batchId=batch_id, n_lines=len(dfPPG_WOLines.index), rank=orderRank)

    # STRICT RULE: First WO line must succeed (with 2 tries max, including a one-time re-fetch)
    first_line_ok = False

    for index, row in dfPPG_WOLines.iterrows():
        retry_count = 0
        success = False
        wol_id = row['WorkorderlineId']

        while not success and retry_count < 2:
            t0 = time.perf_counter()
            x = update_work_order_line_itempathhelper(
                work_order_line_id=wol_id,
                batch_id=batch_id,
                handling_rank=orderRank
            )
            elapsed = round((time.perf_counter() - t0) * 1000, 1)
            print(x)  # preserved

            ok = hasattr(x, 'status_code') and x.status_code == 200
            log_event("batch.assign.try",
                      order=orderName, wol=wol_id, batchId=batch_id,
                      attempt=(retry_count+1), ok=bool(ok), ms=elapsed,
                      http_status=(getattr(x, "status_code", None) if x is not None else None))

            if ok:
                success = True
                if index == 0:
                    first_line_ok = True
                logging.info(f"Successfully updated WO line {wol_id}")
                break

            # If first attempt on FIRST line failed, re-fetch WO lines once and retry with new first id
            if index == 0 and retry_count == 0:
                time.sleep(0.2)  # tiny settle
                dfPPG_WOLines_ref = run_getOpenPPGWOLines_specific(order_id=dfPPG_order.iloc[0]['MasterorderId'])
                log_event("ppg.wolines.snapshot", order=orderName, stage="addOrderToBatch.retry_refetch",
                          profile=_df_profile(dfPPG_WOLines_ref, ["WorkorderlineId", "MasterorderId"]))
                if not dfPPG_WOLines_ref.empty:
                    wol_id = dfPPG_WOLines_ref.iloc[0]['WorkorderlineId']
                    log_event("wol.assign.retry_refetch", order=orderName, wol_new=wol_id)

            retry_count += 1
            if retry_count < 2:
                logging.warning(f"Failed to update WO line {wol_id}, attempt {retry_count}/2")
                time.sleep(1)  # small wait
            else:
                logging.error(f"Failed to update WO line {wol_id} after 2 attempts")

        # If FIRST line didn’t stick, abort this order; do not advance rank upstream
        if index == 0 and not success:
            log_event("batch.assign.abort_first_line", order=orderName, batch=batchName, batchId=batch_id)
            logging.warning(f"Order {orderName}: first WO line failed after retries; NOT adding this order (rank preserved).")
            return False

        # For lines 2..N, we keep going even if they fail (by requirement)

    log_event("batch.assign.end", order=orderName, batch=batchName, batchId=batch_id, log_file=get_current_log_file())
    logging.info(f"Order {orderName} has been added to {batchName}")

    return True
