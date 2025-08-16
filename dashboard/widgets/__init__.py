# dashboard/widgets/__init__.py

# Legacy widgets (wherever they live now)
from .legacy import (      # rename from widgets.py -> widgets/legacy.py
    overlay_years_chart,
    weekly_yoy_table,
    pie_chart,
    kpi_card,
    filter_meaningful_rows,   # if you still use it elsewhere
)

# Engagement widgets
from .engagement import stat_row, cadence_bars, people_table

# Optional: core helpers if you added them
try:
    from .core import ranged_table
except Exception:
    pass
