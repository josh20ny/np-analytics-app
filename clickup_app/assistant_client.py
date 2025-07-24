# clickup_app/assistant_client.py

import os
import time
import openai
import json
from weekly_summary.data_access import (
    fetch_all_with_yoy,
    fetch_all_mailchimp_rows_for_latest_week,
    fetch_records_for_date,
    fetch_records_for_range,
    aggregate_total_attendance
)
from weekly_summary.formatter import format_summary

openai.api_key = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = os.getenv("OPENAI_ASSISTANT_ID")


def run_assistant_with_tools(prompt: str) -> str:
    print("🧠 Assistant input:", prompt)
    start = time.time()

    thread = openai.beta.threads.create()
    openai.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=prompt
    )

    run = openai.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID
    )

    for _ in range(30):
        run = openai.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if run.status == "completed":
            break
        elif run.status == "requires_action":
            tool_outputs = []
            for call in run.required_action.submit_tool_outputs.tool_calls:
                function_name = call.function.name
                args = eval(call.function.arguments)
                print(f"🌐 Calling tool: {function_name} with args: {args}")
                result = call_tool_function(function_name, args)
                tool_outputs.append({
                    "tool_call_id": call.id,
                    "output": result
                })
            run = openai.beta.threads.runs.submit_tool_outputs(
                thread_id=thread.id,
                run_id=run.id,
                tool_outputs=tool_outputs
            )
        else:
            time.sleep(1)

    messages = openai.beta.threads.messages.list(thread_id=thread.id)
    for m in reversed(messages.data):
        if m.role == "assistant":
            print("🧠 Assistant finished in", round(time.time() - start, 2), "s")
            return m.content[0].text.value

    return "(No reply from assistant)"


def call_tool_function(function_name: str, args: dict) -> str:
    latest = fetch_all_with_yoy()

    if function_name == "getCheckinsSummary":
        return format_summary(latest)
    elif function_name == "getMailchimpSummary":
        mailchimp_rows = fetch_all_mailchimp_rows_for_latest_week()
        return format_summary(latest, mailchimp_rows)
    elif function_name == "getYouTubeSummary":
        return format_summary(latest)
    elif function_name == "getAdultAttendance":
        return format_summary(latest)
    # ── New table queries ────────────────────────────────────────
    elif function_name in {
        "getAdultAttendance",
        "getGroupsSummary",
        "getInsideOutAttendance",
        "getLivestreams",
        "getMailchimpWeeklySummary",
        "getTransitAttendance",
        "getUpstreetAttendance",
        "getWaumbaLandAttendance",
        "getWeeklyYouTubeSummary",
    }:
        # Map the tool name to the TABLES key
        key_map = {
            "getAdultAttendance":          "AdultAttendance",
            "getGroupsSummary":            "GroupsSummary",
            "getInsideOutAttendance":      "InsideOutAttendance",
            "getLivestreams":              "Livestreams",
            "getMailchimpWeeklySummary":   "MailchimpSummary",
            "getTransitAttendance":        "TransitAttendance",
            "getUpstreetAttendance":       "UpStreetAttendance",
            "getWaumbaLandAttendance":     "WaumbaLandAttendance",
            "getWeeklyYouTubeSummary":     "WeeklyYouTubeSummary",
        }
        table_key = key_map[function_name]

        # Decide between a single-date or a date-range query
        if "start_date" in args and "end_date" in args:
            rows = fetch_records_for_range(table_key, args["start_date"], args["end_date"])
        elif "date" in args:
            rows = fetch_records_for_date(table_key, args["date"])
        # ── Compare two adult–attendance ranges ──────────────────────────────
        elif function_name == "compareAdultAttendance":
            # Expect args: year1, year2, month (1–12), or explicit start/end dates
            if {"year1","year2","month"} <= args.keys():
                import calendar
                y1, y2, m = args["year1"], args["year2"], args["month"]
                # build ISO ranges
                sd1 = f"{y1:04d}-{m:02d}-01"
                ed1 = f"{y1:04d}-{m:02d}-{calendar.monthrange(y1,m)[1]}"
                sd2 = f"{y2:04d}-{m:02d}-01"
                ed2 = f"{y2:04d}-{m:02d}-{calendar.monthrange(y2,m)[1]}"
            elif {"start1","end1","start2","end2"} <= args.keys():
                sd1, ed1 = args["start1"], args["end1"]
                sd2, ed2 = args["start2"], args["end2"]
            else:
                return "Missing required args: either year1/year2/month or start1/end1/start2/end2."

            total1 = aggregate_total_attendance("AdultAttendance", sd1, ed1)
            total2 = aggregate_total_attendance("AdultAttendance", sd2, ed2)
            delta  = total2 - total1
            pct    = (delta / total1 * 100) if total1 else None

            result = {
                "range1": {"start": sd1, "end": ed1, "total": total1},
                "range2": {"start": sd2, "end": ed2, "total": total2},
                "difference": delta,
                "percent_change": round(pct,1) if pct is not None else None
            }
            return json.dumps(result)
        else:
            return f"Missing date or start_date/end_date in args: {args}"

        # Return JSON so the Assistant can format it however you like
        return json.dumps(rows, default=str)
    else:
        return f"Unknown tool: {function_name}"

