# app/services/ga4.py
from typing import Optional, Dict, List
from google.analytics.data_v1beta import (
    BetaAnalyticsDataClient, DateRange, RunReportRequest, Dimension, Metric
)
from google.analytics.data_v1beta.types import (
    FilterExpression, Filter, FilterExpressionList
)
from google.oauth2 import service_account
from app.config import GA4_PROPERTY_ID, GOOGLE_ADC_PATH

def client() -> BetaAnalyticsDataClient:
    creds = service_account.Credentials.from_service_account_file(GOOGLE_ADC_PATH)
    return BetaAnalyticsDataClient(credentials=creds)

def _date_range(start_yyyy_mm_dd: str, end_yyyy_mm_dd: str) -> DateRange:
    return DateRange(start_date=start_yyyy_mm_dd, end_date=end_yyyy_mm_dd)

def run_report(
    dimensions: List[str],
    metrics: List[str],
    start_date: str,
    end_date: str,
    dimension_filters: Optional[Dict[str, List[str]]] = None,
):
    dims = [Dimension(name=d) for d in dimensions]
    mets = [Metric(name=m) for m in metrics]

    dim_filter = None
    if dimension_filters:
        exprs: List[FilterExpression] = []
        for dim_name, values in dimension_filters.items():
            exprs.append(
                FilterExpression(
                    filter=Filter(
                        field_name=dim_name,
                        in_list_filter=Filter.InListFilter(values=values),
                    )
                )
            )
        if len(exprs) == 1:
            dim_filter = exprs[0]
        else:
            dim_filter = FilterExpression(
                and_group=FilterExpressionList(expressions=exprs)
            )

    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        date_ranges=[_date_range(start_date, end_date)],
        dimensions=dims,
        metrics=mets,
        dimension_filter=dim_filter,
    )
    resp = client().run_report(req)

    out = []
    for row in resp.rows:
        d = {}
        for i, dim in enumerate(dimensions):
            d[dim] = row.dimension_values[i].value
        for j, met in enumerate(metrics):
            val = row.metric_values[j].value
            try:
                d[met] = float(val)
            except ValueError:
                d[met] = val
        out.append(d)
    return out
