import time
from datetime import datetime, timezone

import boto3

from config import load_settings
from queries import build_queries
from insights import run_queries_batched
from emailer import build_email_bodies, send_email

MONTH_NAMES_DE = [
    "Januar", "Februar", "März", "April", "Mai", "Juni",
    "Juli", "August", "September", "Oktober", "November", "Dezember",
]


def get_last_month_range():
    now = datetime.now(timezone.utc)
    first_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if first_this_month.month == 1:
        first_last_month = first_this_month.replace(year=first_this_month.year - 1, month=12)
    else:
        first_last_month = first_this_month.replace(month=first_this_month.month - 1)

    return first_last_month, first_this_month


def lambda_handler(event, context):
    settings = load_settings()

    logs_client = boto3.client("logs", region_name=settings.aws_region)
    ses_client = boto3.client("ses", region_name=settings.aws_region)

    start_dt, end_dt = get_last_month_range()
    month_name = MONTH_NAMES_DE[start_dt.month - 1]
    month_label_pretty = f"{month_name} {start_dt.year}"
    month_label = start_dt.strftime("%Y-%m")

    queries = build_queries(settings.blocked_ips)

    t0 = time.time()
    kpi_results = run_queries_batched(
        logs_client,
        queries=queries,
        log_group_names=settings.log_group_names,
        start_dt=start_dt,
        end_dt=end_dt,
        concurrency=settings.insights_concurrency,
        max_retries=settings.insights_max_retries,
    )
    t1 = time.time()

    print(
        f"[KPI SUMMARY] month={month_label}, segments={len(kpi_results)}, "
        f"duration_s={t1 - t0:.2f}, concurrency={settings.insights_concurrency}, max_retries={settings.insights_max_retries}"
    )

    text_body, html_body = build_email_bodies(
        month_label_pretty=month_label_pretty,
        start_dt=start_dt,
        end_dt=end_dt,
        kpi_results=kpi_results,
    )

    subject = f"{settings.email_subject_prefix} - {month_label_pretty}"

    send_email(
        ses_client,
        subject=subject,
        text_body=text_body,
        html_body=html_body,
        sender=settings.ses_sender,
        recipients=settings.ses_recipients,
    )

    return {
        "statusCode": 200,
        "body": {
            "month": month_label,
            "month_pretty": month_label_pretty,
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
            "kpis": kpi_results,
            "query_runtime_seconds": round(t1 - t0, 2),
        },
    }
