# clickup_app/assistant_client.py

import os
import time
import openai
from datetime import datetime

openai.api_key = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = os.getenv("OPENAI_ASSISTANT_ID")


def run_assistant_with_tools(prompt: str) -> str:
    print("🧠 Assistant input:", prompt)

    # 1. Create a thread
    thread = openai.beta.threads.create()

    # 2. Add the user message
    openai.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=prompt
    )

    # 3. Run the assistant
    run = openai.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID
    )

    # 4. Poll for status
    for i in range(30):
        run = openai.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if run.status == "completed":
            break
        elif run.status == "requires_action":
            # Manually call tool function
            tool_outputs = []
            for call in run.required_action.submit_tool_outputs.tool_calls:
                function_name = call.function.name
                args = eval(call.function.arguments)
                print(f"🌐 Calling tool: {function_name} with args: {args}")

                # Dynamically map function name to real API call
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

    # 5. Retrieve final assistant message
    messages = openai.beta.threads.messages.list(thread_id=thread.id)
    for m in reversed(messages.data):
        if m.role == "assistant":
            return m.content[0].text.value

    return "(No reply from assistant)"


def call_tool_function(function_name: str, args: dict) -> str:
    import requests

    if function_name == "getCheckinsSummary":
        r = requests.get("https://np-analytics-app.onrender.com/planning-center/checkins", params=args)
        return r.text
    elif function_name == "getMailchimpSummary":
        r = requests.get("https://np-analytics-app.onrender.com/mailchimp/weekly-summary")
        return r.text
    elif function_name == "getYouTubeSummary":
        r = requests.get("https://np-analytics-app.onrender.com/youtube/weekly-summary")
        return r.text
    elif function_name == "getAdultAttendance":
        r = requests.get("https://np-analytics-app.onrender.com/attendance/process-sheet")
        return r.text
    else:
        return f"Unknown tool: {function_name}"
