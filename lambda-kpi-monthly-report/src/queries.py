from typing import Dict, List


def build_ip_filter_not_equals(field_name: str, blocked_ips: List[str]) -> str:
    """
    Dashboard-style:
      | filter not ( field = "ip1" or field = "ip2" ... )
    """
    if not blocked_ips:
        return ""

    conds = [f'{field_name} = "{ip}"' for ip in blocked_ips]
    joined = "\n    or ".join(conds)
    return f"\n| filter not (\n    {joined}\n  )"


def build_ip_filter_message_not_like_bracketed(blocked_ips: List[str]) -> str:
    """
    Complaint widget style:
      | filter not ( @message like "[ip]" or ... )
    """
    if not blocked_ips:
        return ""

    conds = [f'@message like "[{ip}]"' for ip in blocked_ips]
    joined = "\n    or ".join(conds)
    return f"\n| filter not (\n    {joined}\n  )"


def build_queries(blocked_ips: List[str]) -> Dict[str, str]:
    """
    Returns dict[segment] -> Logs Insights query.
    Query returns:
      - Hits
      - UniqueVisitors
    Aligned with the provided dashboard queries.
    """
    ip_mdc = lambda: build_ip_filter_not_equals("mdc_ip", blocked_ips)
    ip_nginx = lambda: build_ip_filter_not_equals("loggingClientIp", blocked_ips)
    ip_msg = lambda: build_ip_filter_message_not_like_bracketed(blocked_ips)

    return {
        "Dashboard (DSB)": f"""
filter @logStream like 'be-account/be-account'
| filter logger = "com.integritynext.usermanagement.domain.supplier.SupplierController"
| filter @message like /Listed .* sustainability assessment statistics for requester/
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "Compliance Profile (CP)": f"""
filter @logStream like 'be-suppliernet/be-suppliernet'
| filter @message like "photos for supplier"
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "Social Media Profile (SMP)": f"""
filter @logStream like 'be-suppliernet/be-suppliernet'
| filter @message like /Loaded .* critical articles for supplier/
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "Compliance Workspace (WS)": f"""
filter @logStream like 'be-questionnaire/be-questionnaire'
| filter @message like /GET \\/questionnaire\\/api\\/topics\\/compliance-status/
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "SupplyChain": f"""
filter @logStream like 'be-supplychain/be-supplychain'
| filter @message like "[SupplyChainController.getSupplyChains] [SUCCESS]"
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "Alerts": f"""
filter @logStream like 'be-alerts/be-alerts'
| filter @message like /GET \\/alerts\\/api\\/alerts\\?pageNum\\=0\\&pageSize\\=100/
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "Actions (Customer)": f"""
filter @logStream like 'be-alerts/be-alerts'
| filter @message like "Loading customer actions page content"
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "Actions (Supplier)": f"""
filter @logStream like 'be-alerts/be-alerts'
| filter @message like "Loading supplier actions page content"
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "Complaint": f"""
filter @logStream like 'be-complaint/be-complaint'
| filter @message like "[GET /complaint/api/complaints?pageNum=0&pageSize=100 [ComplaintController.loadComplaints(..)]] [SUCCESS]"
{ip_msg()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "CBAM-Dashboard(neu)": f"""
filter @logStream like 'be-cbam-dashboard'
| filter @message like /GET \\/cbam\\-dashboard\\/api\\/filters \\[CbamFilterController\\.getFilters\\(\\.\\.\\)\\]\\] \\[SUCCESS\\] \\[.*\\]/
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "EUDR-Dashboard": f"""
filter @logStream like 'be-eudr-dashboard'
| filter @message like /GET \\/eudr\\-dashboard\\/api\\/customers\\/shipments\\?pageNum=0\\&pageSize=.* \\[EudrCustomerShipmentController\\.loadShipments\\(\\.\\.\\)\\]\\] \\[SUCCESS\\] \\[.*\\]/
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "CSRD-Dashboard(ESRS)": f"""
filter @logStream like 'be-esrs-dashboard'
| filter @message like /Loading ESRS topic years/
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
        "SCV-Dashboard(AI-Supply-Chain)": f"""
filter @logStream like 'fe-app/fe-app/'
| parse @message "[*] [*] [*] [*] [*] [*] [*]" as loggingTime, loggingUri, loggingResponseCode, loggingELBIp, loggingClientIp, loggingType, loggingHost
| filter loggingUri = 'GET /ai-supply-chains HTTP/1.1'
{ip_nginx()}
| stats count() as Hits, count_distinct(loggingClientIp) as UniqueVisitors
""",
        "Carbon-Dashboard": f"""
filter @logStream like 'be-carbon-dashboard'
| filter @message like /GET \\/carbon\\-dashboard\\/api\\/filters\\/years \\[CarbonFilterController\\.getAvailableYears\\(\\.\\.\\)\\]\\] \\[SUCCESS\\] \\[.*\\]/
{ip_mdc()}
| stats count() as Hits, count_distinct(mdc_ip) as UniqueVisitors
""",
    }
