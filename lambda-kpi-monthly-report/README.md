# lambda-kpi-monthly-report

AWS Lambda, die monatliche KPI-Zugriffszahlen per CloudWatch Logs Insights ermittelt und per SES versendet.

## Handler
- `handler.lambda_handler`

## Environment Variables
Required:
- `LOG_GROUP_NAMES` (comma-separated)
- `SES_SENDER`
- `SES_RECIPIENTS` (comma-separated)

Optional:
- `REGION` (default: eu-west-1)
- `BLOCKED_IPS` (comma-separated)
- `EMAIL_SUBJECT_PREFIX` (default: IntegrityNext KPI Monatsreport)
- `INSIGHTS_CONCURRENCY` (default: 5)
- `INSIGHTS_MAX_RETRIES` (default: 8)

## IAM Permissions (Minimum)
- `logs:StartQuery`
- `logs:GetQueryResults`
- `ses:SendEmail`
- plus read access to the CloudWatch log groups queried

