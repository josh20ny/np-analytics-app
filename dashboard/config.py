from widgets import overlay_years_chart, weekly_yoy_table, pie_chart, kpi_card, pie_chart_from_provider
from widgets.engagement import stat_row, cadence_bars_v2, people_table, matrix_table
from services.engagement import get_recent_engagement, get_cadence_summary, get_lapsed_people, get_back_door_summary, get_downshifts_people, get_new_nla_people, get_downshifts_from_pie, get_downshift_flow_table

# Mapping of tabs to widget definitions
# Each widget: loader=(table_name, date_col, value_col), widget=function, args=dict
TAB_CONFIG = {
    "Adult Attendance": [
        {"loader": ("adult_attendance", "date", "total_attendance"),
         "widget": overlay_years_chart,
         "args": {"title": "Adult Attendance by Year"}},
        {"loader": ("adult_attendance", "date", "total_attendance"),
         "widget": weekly_yoy_table,
         "args": {"title": "Adult Attendance YoY by Week"}},
        {"loader": ("adult_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Service Time Distribution"}},
        {"loader": ("groups_summary", "date", "number_of_groups"),
         "widget": kpi_card,
         "args": {"label": "Total Groups"}},
    ],

    "Waumba Land Attendance": [
        {"loader": ("waumbaland_attendance", "date", "total_attendance"),
         "widget": overlay_years_chart,
         "args": {"title": "Waumba Land Attendance by Year"}},
        {"loader": ("waumbaland_attendance", "date", "total_attendance"),
         "widget": weekly_yoy_table,
         "args": {"title": "Waumba Land YoY by Week"}},
        {"loader": ("waumbaland_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Service Time Distribution"}},
        {"loader": ("waumbaland_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Gender Distribution"}},
        {"loader": ("waumbaland_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Age Distribution"}},
    ],

    "UpStreet Attendance": [
        {"loader": ("upstreet_attendance", "date", "total_attendance"),
         "widget": overlay_years_chart,
         "args": {"title": "UpStreet Attendance by Year"}},
        {"loader": ("upstreet_attendance", "date", "total_attendance"),
         "widget": weekly_yoy_table,
         "args": {"title": "UpStreet YoY by Week"}},
        {"loader": ("upstreet_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Service Time Distribution"}},
        {"loader": ("upstreet_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Gender Distribution"}},
        {"loader": ("upstreet_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Grade Distribution"}},
    ],

    "Transit Attendance": [
        {"loader": ("transit_attendance", "date", "total_attendance"),
         "widget": overlay_years_chart,
         "args": {"title": "Transit Attendance by Year"}},
        {"loader": ("transit_attendance", "date", "total_attendance"),
         "widget": weekly_yoy_table,
         "args": {"title": "Transit YoY by Week"}},
        {"loader": ("transit_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Service Time Distribution"}},
        {"loader": ("transit_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Gender Distribution"}},
        {"loader": ("transit_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Grade Distribution"}},
    ],

    "InsideOut Attendance": [
        {"loader": ("insideout_attendance", "date", "total_attendance"),
         "widget": overlay_years_chart,
         "args": {"title": "InsideOut Attendance by Year"}},
        {"loader": ("insideout_attendance", "date", "total_attendance"),
         "widget": weekly_yoy_table,
         "args": {"title": "InsideOut YoY by Week"}},
        {"loader": ("insideout_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Service Time Distribution"}},
        {"loader": ("insideout_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Gender Distribution"}},
        {"loader": ("insideout_attendance", "date", None),
         "widget": pie_chart,
         "args": {"title": "Grade Distribution"}},
    ],

    "YouTube": [
        {"loader": ("livestreams", "published_at", "initial_views"),
         "widget": overlay_years_chart,
         "args": {"title": "Initial Livestream Views by Year"}},
        {"loader": ("weekly_youtube_summary", "week_end", "total_views"),
         "widget": overlay_years_chart,
         "args": {"title": "Weekly Total Views by Year"}},
        {"loader": ("livestreams", "published_at", "initial_views"),
         "widget": weekly_yoy_table,
         "args": {"title": "Livestream views YoY by Week"}},
    ],

    "Mailchimp": [
        {"loader": ("mailchimp_weekly_summary", "week_end", "avg_open_rate"),
         "widget": weekly_yoy_table,
         "args": {"title": "Open Rate YoY by Week"}},
        {"loader": ("mailchimp_weekly_summary", "week_end", "avg_click_rate"),
         "widget": weekly_yoy_table,
         "args": {"title": "Click Rate YoY by Week"}},
    ],

    "Website": [],

    "Giving": [
        {"loader": ("weekly_giving_summary", "week_end", "total_giving"),
         "widget": overlay_years_chart,
         "args": {"title": "Total Giving by Year"}},
        {"loader": ("weekly_giving_summary", "week_end", "total_giving"),
         "widget": weekly_yoy_table,
         "args": {"title": "Total Giving YoY by Week"}},
    ],

    "Engagement": [
        {"loader": ("__service__", "ignored", None),
         "widget": stat_row,
         "args": {"title": "This Week", "provider": get_recent_engagement}},

        {
        "loader": ("__service__", "ignored", None),
        "widget": cadence_bars_v2,
        "args": {
            "title": "Giving Cadence Buckets",
            "provider": get_cadence_summary,
            "signals": ("give",)
        },
        },
        {
        "loader": ("__service__", "ignored", None),
        "widget": cadence_bars_v2,
        "args": {
            "title": "Attendance Cadence Buckets",
            "provider": get_cadence_summary,
            "signals": ("attend",)
        },
        },

        {"loader": ("__service__", "ignored", None),
         "widget": people_table,
         "args": {"title": "Lapsed (newly flagged)", "provider": get_lapsed_people, "limit": 100}},
                 {"loader": ("__service__", "ignored", None),
         "widget": stat_row,
         "args": {"title": "Back Door (Last Sunday)", "provider": get_back_door_summary}},

        # Trend charts pulled straight from DB tables
        {"loader": ("back_door_weekly", "week_end", "bdi"),
         "widget": overlay_years_chart,
         "args": {"title": "Back Door Index by Year"}},

        {"loader": ("back_door_weekly", "week_end", "new_nla_count"),
         "widget": overlay_years_chart,
         "args": {"title": "New 'No Longer Attends' by Week"}},

        # Actionable lists
        {"loader": ("__service__", "ignored", None),
         "widget": people_table,
         "args": {"title": "Down-shifts this Week", "provider": get_downshifts_people, "limit": 200}},

        {"loader": ("__service__", "ignored", None),
         "widget": people_table,
         "args": {"title": "New 'No Longer Attends' (90 days)", "provider": get_new_nla_people, "limit": 200}},
         
        # Pie: where did drops start?
        {"loader": ("__service__", "ignored", None),
        "widget": pie_chart_from_provider,
        "args": {"title": "Down-shifts by Starting Tier", "provider": get_downshifts_from_pie}},

        # Matrix: From → To
        {"loader": ("__service__", "ignored", None),
         "widget": matrix_table,
         "args": {"title": "Down-shift Flow (From → To)", "provider": get_downshift_flow_table}},

    ],
}

# Per-tab raw table filters (applies only to the top "Filtered rows" table)
TABLE_FILTERS = {
    "InsideOut Attendance": {"metric_col": "total_attendance", "min_value": 50},
}
