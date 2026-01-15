import random
import time
from datetime import datetime
from typing import Dict, List, Tuple

from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError


def _is_retryable_error(e: Exception) -> bool:
    if isinstance(e, (EndpointConnectionError, ConnectionClosedError)):
        return True
    if not isinstance(e, ClientError):
        return False

    code = e.response.get("Error", {}).get("Code", "")
    http_status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    retryable_codes = {
        "ThrottlingException",
        "TooManyRequestsException",
        "LimitExceededException",
        "ServiceUnavailableException",
        "InternalFailure",
        "InternalError",
        "RequestTimeout",
    }
    if code in retryable_codes:
        return True
    if http_status in (429, 500, 502, 503, 504):
        return True
    return False


def call_with_retries(fn, *, max_retries: int, base_delay: float = 0.4, max_delay: float = 8.0, what: str = "aws_call"):
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as e:
            if not _is_retryable_error(e) or attempt >= max_retries:
                raise
            sleep_cap = min(max_delay, base_delay * (2 ** attempt))
            sleep_s = random.uniform(0, sleep_cap)
            print(f"[RETRY] {what} attempt={attempt+1}/{max_retries} sleep={sleep_s:.2f}s err={type(e).__name__}")
            time.sleep(sleep_s)
            attempt += 1


def start_query(logs_client, *, query_string: str, log_group_names: List[str], start_dt: datetime, end_dt: datetime, max_retries: int) -> str:
    def _call():
        resp = logs_client.start_query(
            logGroupNames=log_group_names,
            startTime=int(start_dt.timestamp()),
            endTime=int(end_dt.timestamp()),
            queryString=query_string,
        )
        return resp["queryId"]

    return call_with_retries(_call, max_retries=max_retries, what="logs.start_query")


def get_query_results(logs_client, *, query_id: str, max_retries: int) -> dict:
    def _call():
        return logs_client.get_query_results(queryId=query_id)

    return call_with_retries(_call, max_retries=max_retries, what="logs.get_query_results")


def extract_single_row_fields(query_results: dict) -> dict:
    results = query_results.get("results", [])
    if not results:
        return {}
    row = results[0]
    out = {}
    for cell in row:
        field = cell.get("field")
        value = cell.get("value")
        if field and value is not None:
            out[field] = value
    return out


def run_queries_batched(
    logs_client,
    *,
    queries: Dict[str, str],
    log_group_names: List[str],
    start_dt: datetime,
    end_dt: datetime,
    concurrency: int,
    max_retries: int,
) -> Dict[str, Tuple[bool, str, str]]:
    """
    Returns dict[segment] -> (ok, hits_or_error, unique_or_empty)
    """
    items = list(queries.items())
    results: Dict[str, Tuple[bool, str, str]] = {}
    in_flight: Dict[str, str] = {}  # queryId -> segment

    poll_steps = [1, 2, 4, 8, 10]
    poll_idx = 0

    def start_more():
        nonlocal items
        while items and len(in_flight) < concurrency:
            seg, q = items.pop(0)
            try:
                qid = start_query(
                    logs_client,
                    query_string=q,
                    log_group_names=log_group_names,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    max_retries=max_retries,
                )
                in_flight[qid] = seg
            except Exception as e:
                results[seg] = (False, f"start_query failed: {e}", "")

    start_more()

    while in_flight:
        finished = []

        for qid, seg in list(in_flight.items()):
            try:
                resp = get_query_results(logs_client, query_id=qid, max_retries=max_retries)
                status = resp.get("status")

                if status in ("Complete", "Cancelled", "Failed", "Timeout"):
                    if status != "Complete":
                        results[seg] = (False, f"Status {status}", "")
                    else:
                        fields = extract_single_row_fields(resp)
                        hits = fields.get("Hits", "0") if fields else "0"
                        uniq = fields.get("UniqueVisitors", "0") if fields else "0"
                        results[seg] = (True, hits, uniq)
                    finished.append(qid)
            except Exception as e:
                results[seg] = (False, f"get_query_results failed: {e}", "")
                finished.append(qid)

        for qid in finished:
            in_flight.pop(qid, None)

        start_more()

        if in_flight:
            time.sleep(poll_steps[min(poll_idx, len(poll_steps) - 1)])
            poll_idx += 1

    return results
