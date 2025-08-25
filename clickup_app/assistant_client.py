import os
import time
import openai
import json
from clickup_app.assistant_tools import (
    fetch_all_with_yoy,
    fetch_all_mailchimp_rows_for_latest_week,
    fetch_records_for_date,
    fetch_records_for_range,
    aggregate_total_attendance,
    compare_adult_attendance,
)
from app.utils.common import now_cst, get_last_sunday_cst, get_previous_week_dates_cst 

openai.api_key = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = os.getenv("OPENAI_ASSISTANT_ID")

# Define available tools for the Assistant
FUNCTIONS = [
    {
        "name": "compareAdultAttendance",
        "description": "Compare adult attendance year-over-year for two given years and a specific month",
        "parameters": {
            "type": "object",
            "properties": {
                "year1": {"type": "integer", "description": "First year, e.g. 2024"},
                "year2": {"type": "integer", "description": "Second year, e.g. 2025"},
                "month": {"type": "integer", "minimum": 1, "maximum": 12, "description": "Month number (1-12)"},
            },
            "required": ["year1", "year2", "month"],
        },
    },
]


def run_assistant_with_tools(prompt: str) -> str:
    print("🧠 Assistant input:", prompt)
    start = time.time()

    # 🔎 Build time context (CST)
    today_cst = now_cst().date()
    last_sunday = get_last_sunday_cst()
    prev_mon, prev_sun = get_previous_week_dates_cst()  # returns ISO strings
    context = (
        f"[Context] Today is {today_cst} (America/Chicago). "
        f"Most recent Sunday: {last_sunday}. "
        f"Last full week (Mon–Sun): {prev_mon} to {prev_sun}."
    )

    # Create a conversation thread
    thread = openai.beta.threads.create()
    openai.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        # 👇 Prepend the time context before the actual user prompt
        content=context + "\n\n" + prompt
    )

    # Run the assistant with tool support
    run = openai.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID
    )

    # Poll until completed or tool action is required
    for _ in range(30):
        run = openai.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if run.status == "completed":
            break
        if run.status == "requires_action":
            tool_outputs = []
            for call in run.required_action.submit_tool_outputs.tool_calls:
                function_name = call.function.name
                args = json.loads(call.function.arguments)
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

    # Extract the final assistant message
    messages = openai.beta.threads.messages.list(thread_id=thread.id)
    for m in reversed(messages.data):
        if m.role == "assistant":
            print("🧠 Assistant finished in", round(time.time() - start, 2), "s")
            return m.content[0].text.value

    return "(No reply from assistant)"


def call_tool_function(function_name: str, args: dict) -> str:
    # Fetch baseline data
    latest = fetch_all_with_yoy()

    # Handle custom tool calls
    if function_name == "compareAdultAttendance":
        # Delegate to the assistant_tools helper
        result = compare_adult_attendance(
            year1=args.get("year1"),
            year2=args.get("year2"),
            month=args.get("month")
        )
        return json.dumps(result)


    # Table-based queries
    table_tools = {
        "getAdultAttendance": "AdultAttendance",
        "getGroupsSummary": "GroupsSummary",
        "getInsideOutAttendance": "InsideOutAttendance",
        "getLivestreams": "Livestreams",
        "getMailchimpWeeklySummary": "MailchimpSummary",
        "getTransitAttendance": "TransitAttendance",
        "getUpstreetAttendance": "UpStreetAttendance",
        "getWaumbaLandAttendance": "WaumbaLandAttendance",
        "getWeeklyYouTubeSummary": "WeeklyYouTubeSummary",
        "getWeeklyGivingSummary":    "WeeklyGivingSummary",
        "getServingVolunteersWeekly": "ServingVolunteersWeekly",
    }

    if function_name in table_tools:
        key = table_tools[function_name]
        # Determine query type
        if "start_date" in args and "end_date" in args:
            rows = fetch_records_for_range(key, args["start_date"], args["end_date"])
        elif "date" in args:
            rows = fetch_records_for_date(key, args["date"])
        else:
            return f"Missing date or range parameters for {function_name}: {args}"
        return json.dumps(rows, default=str)

    return f"Unknown tool: {function_name}"


