import streamlit as st
from lib.db import fetch_users_all, set_user_role, set_user_active, approve_user
import os
from lib.emailer import send_email

ROLES = ["viewer", "admin", "owner"]

def admin_panel():
    st.title("🛠️ Admin")
    st.caption("Manage users and access levels")

    users = fetch_users_all()
    if not users:
        st.info("No users yet.")
        return

    st.subheader("Users")
    st.dataframe(
        [{k: r[k] for k in ("email","username","name","role","is_active","is_verified","created_at","verified_at")}
         for r in users],
        use_container_width=True,
    )

    st.divider()
    st.subheader("Manage a user")

    emails = [u["email"] for u in users]
    selected = st.selectbox("Pick a user", emails)
    current = next((u for u in users if u["email"] == selected), None)
    if not current: return

    col1, col2, col3 = st.columns(3)
    with col1:
        new_role = st.selectbox("Role", ROLES, index=ROLES.index(current.get("role","viewer")))
        if st.button("Save role", key="save_role"):
            set_user_role(selected, new_role)
            st.success(f"Role updated to {new_role}")

    with col2:
        active_lbl = "Deactivate" if current["is_active"] else "Activate"
        if st.button(active_lbl, key="toggle_active"):
            set_user_active(selected, not current["is_active"])
            st.success(f"{'Activated' if not current['is_active'] else 'Deactivated'}")

    with col3:
        if not current["is_verified"] and st.button("Approve (verify)", key="approve"):
            approve_user(selected)
            st.success("User verified")

    st.divider()
    with st.expander("✉️ Email tools"):
        backend = os.getenv("EMAIL_BACKEND", "smtp")
        sender  = os.getenv("EMAIL_FROM") or os.getenv("SMTP_USER") or "not set"
        sg_key  = "present" if os.getenv("SENDGRID_API_KEY") else "missing"
        st.caption(f"Backend: **{backend}** · From: **{sender}** · SendGrid API key: **{sg_key}**")

        with st.form("email_test_form"):
            default_to = (st.session_state.get("auth_user") or {}).get("email", "")
            to = st.text_input("To", value=default_to)
            subject = st.text_input("Subject", value="NP Analytics test")
            body = st.text_area("Body", value="If you got this, email is wired.")
            go = st.form_submit_button("Send test email")
        if go:
            try:
                send_email(to, subject, body)
                st.success(f"Sent to {to}")
            except Exception as e:
                st.error(f"Failed: {e}")
