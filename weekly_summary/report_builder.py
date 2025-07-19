# weekly_summary/report_builder.py

def build_full_report(summary_text: str, logs: list[str], checkins_debug: str = "") -> str:
    divider = "\n" + "=" * 60 + "\n"
    log_lines = "\n".join(f"- {line}" for line in logs)

    # Promote the summary header to heading and footer to subheading
    summary_lines = summary_text.split("\n")
    header_line = summary_lines[0].replace("📊", "# 📊").strip()
    footer_line = summary_lines[-1].replace("🔗", "## 🔗").strip()
    body_lines = summary_lines[1:-1]

    summary_block = "\n".join([header_line] + body_lines + [footer_line])

    if checkins_debug:
        debug_block = "\n\n```\n" + checkins_debug.strip().encode("utf-8").decode("unicode_escape") + "\n```"
    else:
        debug_block = ""

    return f"{summary_block}{debug_block}{divider}📝 *Job Logs*\n{log_lines}"

