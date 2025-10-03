# thread_b_and_a.py
import pandas as pd
import numpy as np
import requests
import os
import threading
import time
from dotenv import load_dotenv
from datetime import datetime
import logging
from pathlib import Path
from .ppgAutoBatchAndAllocating import *
from .orderValidation import convertJerseysToWHSLocation_fast
import pymssql
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from .newOrderValidation import deAllocateOrder, deleteOrder, createOrder, addOrderToBatch, DEFER_CREATE_SENTINEL
from .sqlQueries import *
from app.env import load_environment  # ensure available

env_config = load_environment()

base_url = env_config['ITEMPATH_URL']
auth_token = env_config['ITEMPATH_APPLICATION_TOKEN']

# -------------------------------
# Config for final deferred-creation wait (single-threaded)
# -------------------------------
# You asked for 15–30s; defaults below can be tuned via env without code changes.
DEFER_CREATE_FINAL_WAIT_SEC = float(os.getenv("ITEMPATH_DEFER_CREATE_FINAL_WAIT_SEC", "20"))   # 15–30 recommended
DEFER_CREATE_FINAL_POLL_SEC = float(os.getenv("ITEMPATH_DEFER_CREATE_FINAL_POLL_SEC", "1.5"))  # 1–2s recommended

# -------------------------------
# Helpers
# -------------------------------

def _latest_run_dir() -> Path:
    base_path = Path("data/runs")
    return max(base_path.rglob("metadata.json")).parent

def _safe_split_docnums(val) -> List[str]:
    if isinstance(val, (list, np.ndarray)):
        return [str(x) for x in val]
    s = str(val)
    s = s.replace('[', '').replace(']', '')
    if not s.strip():
        return []
    return [v.strip() for v in s.split(',') if str(v).strip()]

def _append_unique(csv_str: str, new_items: List[str]) -> str:
    """
    Append items to a comma-space separated string if not already present.
    Preserve original order; new items go to the end.
    """
    existing = [s.strip() for s in (csv_str or "").split(",") if s.strip()]
    existing_set = set(existing)
    for x in new_items:
        if x not in existing_set:
            existing.append(x)
            existing_set.add(x)
    return ", ".join(existing)

# -------------------------------
# Prepare batches (unchanged semantics)
# -------------------------------

def prepare_batches_for_ppg(batch_names=None):
    """
    Prepares batches for PPG by filtering and processing data from a Parquet file.
    Args:
        batch_names: List of batch names to filter by. If None, returns all batches.
    """
    # Find the latest run directory
    latest_run = _latest_run_dir()

    # Read batches data from parquet
    df_batches = pd.read_parquet(latest_run / "abs_batches.parquet")

    print(df_batches)

    # Filter by batch names if provided
    if batch_names:
        df_batches = df_batches[df_batches['PPG_BatchName'].isin(batch_names)]
        logging.info(f"Filtered to {len(df_batches)} batches matching provided names")

    # Process DocNums field
    for index, row in df_batches.iterrows():
        docnums = row['DocNums']
        if isinstance(docnums, (list, np.ndarray)):
            updated_docnums = ', '.join(map(str, docnums))
        else:
            updated_docnums = str(docnums).replace('[', '').replace(']', '')
        df_batches.at[index, 'DocNums'] = updated_docnums

    # Save DataFrame to Parquet in the run directory
    filename = f"abs_toBatchInPPG.parquet"
    file_path = latest_run / filename
    df_batches.to_parquet(file_path, index=False)
    logging.info(f"Batches prepared and saved to {file_path}")

    return df_batches

# -------------------------------
# Per-thread worker
# -------------------------------

def _process_single_batch_row(
    row: pd.Series,
    thread_id: int,
    df_sapBOM_preloaded: pd.DataFrame,
    df_ppgPickOrders: pd.DataFrame,
    latest_run: Path,
    output_paths: Dict[int, Path],
    batch_order_counts: Dict[str, int],
    batch_counts_lock: threading.Lock,
    deferred_map: Dict[str, List[str]],
    df_errors_by_thread: Dict[int, pd.DataFrame],
    deferred_create_map: Dict[str, List[str]],
) -> None:
    """
    Processes a single batch row: creates/deletes/deallocates orders, tries to add to batch.
    On add-to-batch failure, defers the SO for a global tail retry.
    On order-create HTTP 500, defers creation for the FINAL pass (no immediate retry to avoid dupes).
    Updates per-thread parquet outputs continuously.
    """
    # Thread-scoped file paths
    out_ok = output_paths[thread_id]
    out_err = latest_run / f"abs_toPrintInSAP_errors_{thread_id}.parquet"

    # Thread-scoped running frames (loaded if exist; otherwise created new)
    try:
        df_ok = pd.read_parquet(out_ok) if out_ok.exists() else pd.DataFrame()
    except Exception:
        df_ok = pd.DataFrame()

    df_errors = df_errors_by_thread.get(thread_id)
    if df_errors is None:
        df_errors = pd.DataFrame()

    # Copy row to avoid SettingWithCopy headaches
    row = row.copy()
    batch_name = row['PPG_BatchName']
    so_list = _safe_split_docnums(row['DocNums'])

    logging.info(f"[T{thread_id}] Processing Batch '{batch_name}' with {len(so_list)} orders")

    # Ensure tracking counter exists
    with batch_counts_lock:
        if batch_name not in batch_order_counts:
            batch_order_counts[batch_name] = 0
        current_rank = batch_order_counts[batch_name] + 1  # next rank to assign

    allocated_arr: List[str] = []
    orders_in_batch = len(so_list)

    for idx, soNum in enumerate(so_list):
        try:
            # Use pre-loaded SAP BOM data for this SO
            df_sap_filtered = df_sapBOM_preloaded[
                df_sapBOM_preloaded['DocNum'].astype(str) == str(soNum)
            ]

            # (Optional) current PPG view; not required here
            df_ppgFiltered = df_ppgPickOrders[
                df_ppgPickOrders['MasterorderName'].astype(str) == str(soNum)
            ]

            # If present in PPG, clean up (dealloc + delete)
            if not df_ppgFiltered.empty:
                deAllocated = deAllocateOrder(orderName=soNum)
                logging.info(f"[T{thread_id}] {soNum} - Deallocated? {deAllocated}")
                deleted = deleteOrder(orderName=soNum)
                logging.info(f"[T{thread_id}] {soNum} - Deleted? {deleted}")

            # Create order from SAP
            created = createOrder(orderName=soNum, df_sapBOM=df_sap_filtered)

            # Handle DEFER_CREATE sentinel (HTTP 500)
            if isinstance(created, dict) and created.get("defer_create"):
                logging.warning(f"[T{thread_id}] Defer CREATE for SO {soNum} in '{batch_name}' (HTTP 500)")
                deferred_create_map.setdefault(batch_name, []).append(soNum)
                # Do NOT attempt add-to-batch now; rank not consumed.
            elif created:
                # Attempt to add to batch at the current rank
                success = addOrderToBatch(orderName=soNum, batchName=batch_name, orderRank=current_rank)
                if success:
                    allocated_arr.append(soNum)
                    # rank consumed only on success
                    with batch_counts_lock:
                        batch_order_counts[batch_name] += 1
                        current_rank = batch_order_counts[batch_name] + 1
                else:
                    # Defer this SO for end-of-run retry on the same batch (at tail)
                    logging.warning(f"[T{thread_id}] Defer add-to-batch for SO {soNum} in '{batch_name}'")
                    deferred_map.setdefault(batch_name, []).append(soNum)
                    # do not consume the rank here
            else:
                # Could not create order; capture error
                logging.error(f"[T{thread_id}] Create order failed for {soNum}")
                error_row = row.copy()
                error_row['soNum'] = soNum
                error_row['reason'] = "create_failed"
                df_errors = pd.concat([df_errors, pd.DataFrame([error_row])], ignore_index=True)

            # Periodic progress logging per batch
            if (idx + 1) % 10 == 0 or (idx + 1) == orders_in_batch:
                logging.info(f"[T{thread_id}]  - Batch {batch_name}: Processed {idx + 1}/{orders_in_batch} orders")

        except Exception as e:
            logging.error(f"[T{thread_id}]  - SO {soNum}: Error - {str(e)}")
            error_row = row.copy()
            error_row['soNum'] = soNum
            error_row['reason'] = f"Exception: {str(e)}"
            df_errors = pd.concat([df_errors, pd.DataFrame([error_row])], ignore_index=True)

        # Persist partial progress for this batch to per-thread parquet
        # Build/refresh a single-row status for this batch in df_ok
        row_out = row.copy()
        row_out['ordersBatched'] = ', '.join(allocated_arr)
        row_out['soCount'] = len(allocated_arr)
        # Upsert by (PPG_BatchName)
        if df_ok.empty:
            df_ok = pd.DataFrame([row_out])
        else:
            # remove any previous for this batch, then append
            df_ok = df_ok[df_ok['PPG_BatchName'] != batch_name]
            df_ok = pd.concat([df_ok, pd.DataFrame([row_out])], ignore_index=True)

        df_ok.to_parquet(out_ok, index=False)
        if not df_errors.empty:
            df_errors.to_parquet(out_err, index=False)

    # Update thread-local error frame cache
    df_errors_by_thread[thread_id] = df_errors

    logging.info(f"[T{thread_id}] Batch '{batch_name}' finished. Success: {len(allocated_arr)}/{len(so_list)}")

# -------------------------------
# Combine parquet outputs
# -------------------------------

def combine_parquet_files(type_suffix=""):
    """
    Combines multiple parquet files into a single file.
    """
    latest_run = _latest_run_dir()
    df_list = []
    output_suffix = "toPrint" if not type_suffix else type_suffix

    for file in latest_run.glob(f'abs_toPrintInSAP*.parquet'):
        if "_errors" not in file.name and file.name.endswith(".parquet"):
            df_list.append(pd.read_parquet(file))

    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        if 'minDocDueDate' in combined_df.columns:
            combined_df = combined_df.sort_values(by=['minDocDueDate'], ascending=[True])
        output_filename = f'abs_toPrintInSAP_{output_suffix}.parquet'
        output_path = latest_run / output_filename
        combined_df.to_parquet(output_path, index=False)
        logging.info(f"Combined results saved to {output_path}")

def combine_error_parquet_files():
    """
    Combines multiple error parquet files into a single file.
    """
    latest_run = _latest_run_dir()
    df_list = []

    for file in latest_run.glob(f'abs_toPrintInSAP_errors_*.parquet'):
        temp_df = pd.read_parquet(file)
        if not temp_df.empty:
            df_list.append(temp_df)

    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        if 'minDocDueDate' in combined_df.columns:
            combined_df = combined_df.sort_values(by=['minDocDueDate'], ascending=[True])
        output_filename = f'abs_toPrintInSAP_errors_toReview.parquet'
        output_path = latest_run / output_filename
        combined_df.to_parquet(output_path, index=False)
        logging.info(f"Combined error results saved to {output_path}")

# -------------------------------
# Apply deferred successes back to per-thread parquets
# -------------------------------

def _apply_deferred_successes_to_parquets(latest_run: Path, deferred_success_map: Dict[str, List[str]]):
    """
    For each batch in deferred_success_map, find its row in any per-thread parquet,
    append newly successful SOs to ordersBatched, and bump soCount.
    This ensures combine_parquet_files() will include tail successes.
    """
    if not deferred_success_map:
        return

    # Scan all per-thread files and update in place
    part_files = sorted([p for p in latest_run.glob("abs_toPrintInSAP_*.parquet") if "_errors" not in p.name])
    if not part_files:
        return

    for pf in part_files:
        try:
            df = pd.read_parquet(pf)
        except Exception:
            continue
        if df.empty or 'PPG_BatchName' not in df.columns:
            continue

        changed = False
        for batch_name, added_sos in deferred_success_map.items():
            mask = (df['PPG_BatchName'] == batch_name)
            if not mask.any():
                continue
            idxs = df.index[mask].tolist()
            for i in idxs:
                existing_csv = df.at[i, 'ordersBatched'] if 'ordersBatched' in df.columns else ""
                new_csv = _append_unique(existing_csv, added_sos)
                if new_csv != existing_csv:
                    df.at[i, 'ordersBatched'] = new_csv
                    # update soCount to reflect number of unique SOs in the csv
                    if 'soCount' in df.columns:
                        count = 0 if not new_csv.strip() else len([s for s in new_csv.split(",") if s.strip()])
                        df.at[i, 'soCount'] = count
                    changed = True

        if changed:
            df.to_parquet(pf, index=False)
            logging.info(f"Updated deferred successes into {pf.name}")

# -------------------------------
# Global deferred tails
# -------------------------------

def _retry_deferred_tail(
    deferred_map: Dict[str, List[str]],
    batch_order_counts: Dict[str, int],
    batch_counts_lock: threading.Lock,
    latest_run: Path,
):
    """
    After all batches are processed, retry adding each deferred SO to its batch.
    We deliberately push to the end: rank = current_success_count + 1.
    Also, we record which SOs finally succeeded and apply them into per-thread parquets.
    """
    if not deferred_map:
        logging.info("No deferred orders to retry (add-to-batch).")
        return

    logging.info(f"Retrying deferred add-to-batch for {sum(len(v) for v in deferred_map.values())} orders...")
    # Keep track of which ones succeeded during tail
    deferred_success_map: Dict[str, List[str]] = defaultdict(list)

    # Deterministic retry order: by batch, then SO
    for batch_name in sorted(deferred_map.keys()):
        for soNum in deferred_map[batch_name]:
            # Ensure the order exists before retry
            df_ppg = run_getOpenPPGPickOrders_specific(orderName=soNum)
            if df_ppg.empty:
                logging.warning(f"[deferred] Skip {soNum} for '{batch_name}' (order missing in PPG)")
                continue

            with batch_counts_lock:
                current_rank = batch_order_counts.get(batch_name, 0) + 1

            success = addOrderToBatch(orderName=soNum, batchName=batch_name, orderRank=current_rank)
            if success:
                with batch_counts_lock:
                    batch_order_counts[batch_name] = batch_order_counts.get(batch_name, 0) + 1
                deferred_success_map[batch_name].append(soNum)
                logging.info(f"[deferred] Added {soNum} to '{batch_name}' at rank {current_rank}")
            else:
                logging.error(f"[deferred] FINAL FAIL adding {soNum} to '{batch_name}'")

    # Reflect all deferred successes into per-thread parquet shards
    _apply_deferred_successes_to_parquets(latest_run, deferred_success_map)

def _final_visibility_wait_for_order(soNum: str) -> bool:
    """
    Single-threaded final pass wait:
    Poll PPG up to DEFER_CREATE_FINAL_WAIT_SEC (every DEFER_CREATE_FINAL_POLL_SEC)
    to see if the order shows up on its own. Returns True if visible.
    """
    t0 = time.time()
    tries = 0
    while (time.time() - t0) < DEFER_CREATE_FINAL_WAIT_SEC:
        tries += 1
        df_ppg = run_getOpenPPGPickOrders_specific(orderName=soNum)
        found = not df_ppg.empty
        logging.info(f"[deferred-create:wait] {soNum} visible={found} try={tries}")
        if found:
            return True
        time.sleep(DEFER_CREATE_FINAL_POLL_SEC)
    return False

def _retry_deferred_create_then_add_tail(
    deferred_create_map: Dict[str, List[str]],
    batch_order_counts: Dict[str, int],
    batch_counts_lock: threading.Lock,
    latest_run: Path,
    df_sapBOM_preloaded: pd.DataFrame,
):
    """
    FINAL pass (runs on a single thread after the pool completes):
      - For each SO that hit HTTP 500 during creation, we FIRST wait up to
        DEFER_CREATE_FINAL_WAIT_SEC (poll every DEFER_CREATE_FINAL_POLL_SEC) to see if it appears.
      - If it appears, skip creation and directly attempt add-to-batch at tail rank.
      - If it doesn't appear, try creating ONCE now, then attempt add-to-batch.
      - Update shard parquets with successes so the combined parquet includes them.
    """
    if not deferred_create_map:
        logging.info("No deferred-creation orders to retry.")
        return

    logging.info(f"Retrying deferred CREATION for {sum(len(v) for v in deferred_create_map.values())} orders...")

    late_success_map: Dict[str, List[str]] = defaultdict(list)

    for batch_name in sorted(deferred_create_map.keys()):
        for soNum in deferred_create_map[batch_name]:
            try:
                # First: single-threaded visibility wait (avoid duplicate creates)
                became_visible = _final_visibility_wait_for_order(soNum)

                created_ok = False
                if became_visible:
                    created_ok = True  # treat as created
                    logging.info(f"[deferred-create] {soNum} appeared during final wait; skipping create.")
                else:
                    # Load SAP BOM for this SO from preloaded frame
                    df_sap_filtered = df_sapBOM_preloaded[
                        df_sapBOM_preloaded['DocNum'].astype(str) == str(soNum)
                    ]
                    # Create once now
                    created = createOrder(orderName=soNum, df_sapBOM=df_sap_filtered)
                    # If createOrder returns DEFER_CREATE_SENTINEL again, we cannot loop forever; mark as fail
                    if isinstance(created, dict) and created.get("defer_create"):
                        logging.error(f"[deferred-create] {soNum} returned defer sentinel again at final pass; skipping.")
                        created_ok = False
                    else:
                        created_ok = bool(created)

                if created_ok:
                    # Add to batch at tail rank
                    with batch_counts_lock:
                        current_rank = batch_order_counts.get(batch_name, 0) + 1

                    success = addOrderToBatch(orderName=soNum, batchName=batch_name, orderRank=current_rank)
                    if success:
                        with batch_counts_lock:
                            batch_order_counts[batch_name] = batch_order_counts.get(batch_name, 0) + 1
                        late_success_map[batch_name].append(soNum)
                        logging.info(f"[deferred-create] Created/Visible+Added {soNum} to '{batch_name}' at rank {current_rank}")
                    else:
                        logging.error(f"[deferred-create] Created/Visible {soNum} but add-to-batch failed for '{batch_name}'")
                else:
                    logging.error(f"[deferred-create] FINAL FAIL creating {soNum} for '{batch_name}'")

            except Exception as e:
                logging.error(f"[deferred-create] Exception for {soNum} in '{batch_name}': {e}")

    # Update shard parquets with late successes
    _apply_deferred_successes_to_parquets(latest_run, late_success_map)

# -------------------------------
# Dynamic FIFO worker pool runner (back-compat wrapper)
# -------------------------------

def batch_and_allocate_orders_optimized(df: pd.DataFrame, thread_id: int, df_sapBOM_preloaded: pd.DataFrame, df_ppgPickOrders: pd.DataFrame):
    """
    Kept for backward compatibility. This single-thread wrapper delegates to the global
    run_specificBatches_optimized which now handles the FIFO worker pool.
    Here, we still process given df synchronously in this thread id.
    """
    latest_run = _latest_run_dir()
    output_filename = f"abs_toPrintInSAP_{thread_id}.parquet"
    output_file_path = latest_run / output_filename
    output_errFileName = f"abs_toPrintInSAP_errors_{thread_id}.parquet"
    output_errFilePath = latest_run / output_errFileName

    # Add necessary columns
    df = df.copy()
    df['ordersBatched'] = ""
    df['soCount'] = 0
    df_errors = pd.DataFrame()

    # Prepare counters and shared structs (local to this wrapper)
    batch_order_counts: Dict[str, int] = defaultdict(int)
    batch_counts_lock = threading.Lock()
    deferred_map: Dict[str, List[str]] = {}
    deferred_create_map: Dict[str, List[str]] = {}
    df_errors_by_thread: Dict[int, pd.DataFrame] = {thread_id: df_errors}
    output_paths = {thread_id: output_file_path}

    # Process each batch row in-order (FIFO for this wrapper)
    for _, row in df.iterrows():
        _process_single_batch_row(
            row=row,
            thread_id=thread_id,
            df_sapBOM_preloaded=df_sapBOM_preloaded,
            df_ppgPickOrders=df_ppgPickOrders,
            latest_run=latest_run,
            output_paths=output_paths,
            batch_order_counts=batch_order_counts,
            batch_counts_lock=batch_counts_lock,
            deferred_map=deferred_map,
            df_errors_by_thread=df_errors_by_thread,
            deferred_create_map=deferred_create_map,
        )

    # First: retry deferred add-to-batch tail
    _retry_deferred_tail(
        deferred_map=deferred_map,
        batch_order_counts=batch_order_counts,
        batch_counts_lock=batch_counts_lock,
        latest_run=latest_run
    )

    # Second (FINAL): deferred creation wait -> create once if needed -> add tail
    _retry_deferred_create_then_add_tail(
        deferred_create_map=deferred_create_map,
        batch_order_counts=batch_order_counts,
        batch_counts_lock=batch_counts_lock,
        latest_run=latest_run,
        df_sapBOM_preloaded=df_sapBOM_preloaded
    )

    logging.info(f"[Thread {thread_id}] Completed processing {len(df)} batches")
    return df, output_file_path

# -------------------------------
# Public runner (FIFO, dynamic workers)
# -------------------------------

def run_specificBatches_optimized(batch_names, num_threads):
    """
    Optimized FIFO worker-pool version that:
      1) Pre-loads SAP BOM + PPG orders once
      2) Builds a shared FIFO queue of batch rows (optionally sorted by minDocDueDate, then name)
      3) Spawns dynamic workers that each pull the next batch when ready
      4) Defers failed add-to-batch orders and retries them at the very end (tail rank)
      5) Defers HTTP 500 create errors for a SINGLE final create+add pass
      6) In the final pass, waits up to DEFER_CREATE_FINAL_WAIT_SEC polling every DEFER_CREATE_FINAL_POLL_SEC
         before attempting a single creation, then adds at tail rank
      7) Applies deferred successes into shard parquets BEFORE combining them
      8) Combines per-thread parquet outputs
    """
    logging.info("Starting optimized batch processing")
    logging.info(f"Processing batches: {batch_names}")

    # Prepare batches
    df_batches = prepare_batches_for_ppg(batch_names)
    if df_batches.empty:
        logging.warning("No matching batches found")
        return False

    total_batches = len(df_batches)
    logging.info(f"Total batches to process: {total_batches}")

    # Extract ALL doc numbers at once (for a single SAP BOM query)
    all_formatted_docs = []
    for _, row in df_batches.iterrows():
        doc_nums = _safe_split_docnums(row['DocNums'])
        all_formatted_docs.extend([f"'{num}'" for num in doc_nums])

    # Run the SAP query ONCE with all documents
    logging.info(f"Loading SAP BOM data for {len(all_formatted_docs)} documents...")
    df_sapBOM_all = run_sapBOMQuery("(" + ','.join(all_formatted_docs) + ")")
    logging.info(f"Loaded {len(df_sapBOM_all)} SAP BOM records")

    logging.info(f"Loading PPG Pick data...")
    df_ppgPickOrders = run_getOpenPPGPickOrders()
    logging.info(f"Loaded {len(df_ppgPickOrders)} PPG orders snapshot")

    # Build a shared FIFO queue of batch rows (deterministic order)
    q: "queue.Queue[pd.Series]" = queue.Queue()

    # Stable deterministic order: by due date then batch name (fallback to name)
    if 'minDocDueDate' in df_batches.columns:
        df_batches_sorted = df_batches.sort_values(
            by=['minDocDueDate', 'PPG_BatchName'],
            ascending=[True, True],
            kind='stable'
        )
    else:
        df_batches_sorted = df_batches.sort_values(by=['PPG_BatchName'], kind='stable')

    for _, row in df_batches_sorted.iterrows():
        q.put(row)  # FIFO: first put, first out

    # Shared structures
    latest_run = _latest_run_dir()
    # Pre-allocate per-thread output file paths (suffix by worker id 1..N)
    output_paths: Dict[int, Path] = {}
    for i in range(1, min(num_threads, total_batches) + 1):
        output_paths[i] = latest_run / f"abs_toPrintInSAP_{i}.parquet"

    batch_order_counts: Dict[str, int] = defaultdict(int)  # successful orders per batch
    batch_counts_lock = threading.Lock()
    deferred_map: Dict[str, List[str]] = {}               # batch -> [soNums] for add failures
    deferred_create_map: Dict[str, List[str]] = {}        # batch -> [soNums] for HTTP 500 create
    df_errors_by_thread: Dict[int, pd.DataFrame] = {}

    # Worker function
    def worker(worker_id: int):
        processed = 0
        while True:
            try:
                row = q.get_nowait()
            except queue.Empty:
                break
            try:
                _process_single_batch_row(
                    row=row,
                    thread_id=worker_id,
                    df_sapBOM_preloaded=df_sapBOM_all,
                    df_ppgPickOrders=df_ppgPickOrders,
                    latest_run=latest_run,
                    output_paths=output_paths,
                    batch_order_counts=batch_order_counts,
                    batch_counts_lock=batch_counts_lock,
                    deferred_map=deferred_map,
                    df_errors_by_thread=df_errors_by_thread,
                    deferred_create_map=deferred_create_map,
                )
                processed += 1
            finally:
                q.task_done()
        logging.info(f"[Worker {worker_id}] done. Batches processed: {processed}")

    # Launch pool
    n_workers = min(num_threads, total_batches)
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = [executor.submit(worker, i + 1) for i in range(n_workers)]
        for i, fut in enumerate(as_completed(futures), start=1):
            try:
                fut.result()
            except Exception as exc:
                logging.error(f"[Worker error] {exc}")

    # All initial processing done -> retry deferred add-to-batch tail
    _retry_deferred_tail(
        deferred_map=deferred_map,
        batch_order_counts=batch_order_counts,
        batch_counts_lock=batch_counts_lock,
        latest_run=latest_run
    )

    # FINAL pass -> single-thread visibility wait, then deferred creation, then add-to-batch tail
    _retry_deferred_create_then_add_tail(
        deferred_create_map=deferred_create_map,
        batch_order_counts=batch_order_counts,
        batch_counts_lock=batch_counts_lock,
        latest_run=latest_run,
        df_sapBOM_preloaded=df_sapBOM_all
    )

    # Combine results (now includes all deferred tail successes because shards are updated)
    combine_parquet_files()
    combine_error_parquet_files()

    logging.info("Batch processing completed")
    return True

def run_specificBatches(batch_names, num_threads=5):
    """
    Keep original function name for backward compatibility
    """
    return run_specificBatches_optimized(batch_names, num_threads)
