# test_assistant.py

import os
import time
import openai
import requests
from dotenv import load_dotenv

load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = os.getenv("OPENAI_ASSISTANT_ID")
APP_BASE_URL = "https://np-analytics-app.onrender.com"  # Change if needed

def call_tool_api(name, args):
    print(f"🌐 Calling tool: {name} with args: {args}")

    if name == "getCheckinsSummary":
        date = args.get("date")
        url = f"{APP_BASE_URL}/planning-center/checkins"
        if date:
            url += f"?date={date}"
        resp = requests.get(url, timeout=30)
        return resp.text

    elif name == "getMailchimpSummary":
        resp = requests.get(f"{APP_BASE_URL}/mailchimp/weekly-summary", timeout=30)
        return resp.text

    elif name == "getYouTubeSummary":
        resp = requests.get(f"{APP_BASE_URL}/youtube/weekly-summary", timeout=30)
        return resp.text

    elif name == "getAdultAttendance":
        resp = requests.get(f"{APP_BASE_URL}/attendance/process-sheet", timeout=30)
        return resp.text

    else:
        return f"❌ Unknown tool: {name}"

def main():
    print(f"🔍 Fetching Assistant {ASSISTANT_ID}...")
    assistant = openai.beta.assistants.retrieve(ASSISTANT_ID)

    print("🛠️  Registered tools:")
    for tool in assistant.tools:
        if hasattr(tool, "function") and hasattr(tool.function, "name"):
            print("  •", tool.function.name)
        else:
            print("  •", type(tool).__name__)

    thread = openai.beta.threads.create()
    print(f"🧵 Thread ID: {thread.id}")

    user_message = "What were our Planning Center check-ins last Sunday?"
    print(f"✉️  User message: {user_message}")

    openai.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=user_message
    )

    run = openai.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID
    )

    # Poll for run status
    for i in range(30):
        print(f"⏳ Checking run status... ({i+1}/30)")
        time.sleep(1)
        run = openai.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if run.status != "in_progress":
            break

    print(f"✅ Run status: {run.status}")

    # Handle tool calls
    if run.status == "requires_action":
        tool_outputs = []
        for tool_call in run.required_action.submit_tool_outputs.tool_calls:
            func_name = tool_call.function.name
            args = eval(tool_call.function.arguments)
            output = call_tool_api(func_name, args)
            tool_outputs.append({
                "tool_call_id": tool_call.id,
                "output": output
            })

        # Submit tool output back to assistant
        run = openai.beta.threads.runs.submit_tool_outputs(
            thread_id=thread.id,
            run_id=run.id,
            tool_outputs=tool_outputs
        )

        # Poll again for final reply
        for i in range(30):
            print(f"⏳ Waiting for final reply... ({i+1}/30)")
            time.sleep(1)
            run = openai.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
            if run.status != "in_progress":
                break

    # Print final assistant message
    messages = openai.beta.threads.messages.list(thread_id=thread.id)
    for msg in messages.data:
        for content in msg.content:
            if content.type == "text":
                print("📨 Assistant reply:\n")
                print(content.text.value)
                return

    print("⚠️ No reply returned.")

if __name__ == "__main__":
    main()
