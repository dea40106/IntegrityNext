import os
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Settings:
    aws_region: str
    log_group_names: List[str]
    ses_sender: str
    ses_recipients: List[str]
    blocked_ips: List[str]
    email_subject_prefix: str
    insights_concurrency: int
    insights_max_retries: int


def _csv_list(value: str) -> List[str]:
    return [x.strip() for x in (value or "").split(",") if x.strip()]


def load_settings() -> Settings:
    aws_region = os.getenv("REGION", "eu-west-1")

    log_group_names = _csv_list(os.getenv("LOG_GROUP_NAMES", ""))
    ses_sender = os.getenv("SES_SENDER", "")
    ses_recipients = _csv_list(os.getenv("SES_RECIPIENTS", ""))
    blocked_ips = _csv_list(os.getenv("BLOCKED_IPS", ""))

    email_subject_prefix = os.getenv(
        "EMAIL_SUBJECT_PREFIX", "IntegrityNext KPI Monatsreport"
    ).strip()

    insights_concurrency = int(os.getenv("INSIGHTS_CONCURRENCY", "5"))
    insights_max_retries = int(os.getenv("INSIGHTS_MAX_RETRIES", "8"))

    missing = []
    if not log_group_names:
        missing.append("LOG_GROUP_NAMES")
    if not ses_sender:
        missing.append("SES_SENDER")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
    if not ses_recipients:
        raise RuntimeError("SES_RECIPIENTS must contain at least one email address.")

    if insights_concurrency < 1:
        insights_concurrency = 1
    if insights_max_retries < 0:
        insights_max_retries = 0

    return Settings(
        aws_region=aws_region,
        log_group_names=log_group_names,
        ses_sender=ses_sender,
        ses_recipients=ses_recipients,
        blocked_ips=blocked_ips,
        email_subject_prefix=email_subject_prefix,
        insights_concurrency=insights_concurrency,
        insights_max_retries=insights_max_retries,
    )
