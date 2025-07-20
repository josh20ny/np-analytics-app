import os
import time
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = os.getenv("OPENAI_ASSISTANT_ID")

def get_reply_from_assistant(message: str) -> str:
    # Create a new thread
    thread = openai.beta.threads.create()

    # Add message to thread
    openai.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=message
    )

    # Run the assistant
    run = openai.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID
    )

    # Wait for the run to finish
    while run.status not in ("completed", "failed", "cancelled"):
        time.sleep(1)
        run = openai.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        print("🤖 Run status:", run.status)
        print("🛠️  Tools used:", run.tools if hasattr(run, "tools") else "None")

    # Get the reply
    messages = openai.beta.threads.messages.list(thread_id=thread.id)
    for msg in messages.data:
        for content in msg.content:
            if content.type == "text":
                print("📨 Assistant text reply:", content.text.value)
                return content.text.value

    return "Sorry, I couldn't generate a response."

