# clickup_app/assistant_client.py

import os
import time
import openai
from weekly_summary.data_access import (
    fetch_all_with_yoy,
    fetch_all_mailchimp_rows_for_latest_week,
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
    else:
        return f"Unknown tool: {function_name}"

