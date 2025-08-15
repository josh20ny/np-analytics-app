import os, re, secrets, string, bcrypt, streamlit as st
import streamlit_authenticator as stauth
from datetime import datetime, timezone
from .db import (
    fetch_active_users, insert_user, update_password, get_user_by_email,
    set_verification, get_verification, mark_verified
)
from .emailer import send_email

try:
    from streamlit_authenticator.utilities.hasher import Hasher  # 0.4.x
except Exception:
    try:
        from streamlit_authenticator.utilities import Hasher      # some builds
    except Exception:
        Hasher = stauth.Hasher

# ---- helpers ----

def _aware(dt):
    return dt if (dt and dt.tzinfo) else (dt.replace(tzinfo=timezone.utc) if dt else None)

def _allowed(email: str) -> bool:
    emails  = {e.strip().lower() for e in os.getenv("PREAUTHORIZED_EMAILS","").split(",") if e.strip()}
    domains = {d.strip().lower() for d in os.getenv("ALLOWED_EMAIL_DOMAINS","").split(",") if d.strip()}
    mail = email.strip().lower()
    if not emails and not domains: return True
    if mail in emails: return True
    return mail.split("@")[-1] in domains

def _credentials_dict():
    users = fetch_active_users()   # ⬅️ only verified users are included
    return {
        "usernames": {
            u["username"]: {
                "email": u["email"],
                "name":  u["name"],
                "password": u["password_hash"],
            } for u in users
        }
    }

def _make_code(n=6):
    return ''.join(secrets.choice(string.digits) for _ in range(n))

def _hash_code(code: str) -> str:
    return bcrypt.hashpw(code.encode(), bcrypt.gensalt()).decode()

def _check_code(code: str, code_hash: str) -> bool:
    try:
        return bcrypt.checkpw(code.encode(), code_hash.encode())
    except Exception:
        return False

def login_gate(title="Login", render_if_unauth=True):
    """Return True if authenticated. If not authed and render_if_unauth=True,
    show the login form; otherwise just return False."""
    creds = _credentials_dict()
    authenticator = stauth.Authenticate(
        creds,
        os.getenv("DASH_COOKIE_NAME", "np_dash"),
        os.getenv("DASH_COOKIE_KEY", "change-me"),
        float(os.getenv("DASH_COOKIE_EXPIRY_DAYS", "14")),
    )

    # 1) Silent cookie check without showing a form
    authenticator.login("unrendered", key="login_silent")

    status   = st.session_state.get("authentication_status")
    username = st.session_state.get("username")
    name     = st.session_state.get("name")

    if status is True:
        # ensure our app-level user object exists
        if "auth_user" not in st.session_state and username:
            email = creds["usernames"][username]["email"]
            st.session_state["auth_user"] = {"username": username, "name": name, "email": email}
        # logout only (no login UI)
        authenticator.logout("Logout", "sidebar")
        st.sidebar.caption(f"Signed in as **{name or username}**")
        return True

    if render_if_unauth:
        # 2) Show the visible login form only when NOT authed
        authenticator.login(
            "main",
            fields={"Form name": title, "Username": "Username", "Password": "Password", "Login": "Sign in"},
            clear_on_submit=True,
            key="login_visible",
        )
        status = st.session_state.get("authentication_status")
        if status is True:
            username = st.session_state.get("username"); name = st.session_state.get("name")
            email = creds["usernames"][username]["email"]
            st.session_state["auth_user"] = {"username": username, "name": name, "email": email}
            authenticator.logout("Logout", "sidebar")
            st.sidebar.caption(f"Signed in as **{name or username}**")
            return True

    return False

def registration_panel():
    if st.session_state.get("authentication_status") is True:
        return
    if os.getenv("ALLOW_SIGNUPS","false").lower() != "true":
        return

    with st.expander("🔐 Request access (self-register)"):
        with st.form("register"):
            st.write("Only authorized work emails may register.")
            name  = st.text_input("Full name")
            email = st.text_input("Work email").strip().lower()
            username = st.text_input("Username").strip().lower()
            pw1 = st.text_input("Create password", type="password")
            pw2 = st.text_input("Confirm password", type="password")
            submit = st.form_submit_button("Create account")

        if submit:
            if not (name and email and username and pw1 and pw2):
                st.error("All fields are required."); st.stop()
            if pw1 != pw2:
                st.error("Passwords do not match."); st.stop()
            if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
                st.error("Enter a valid email."); st.stop()
            if not _allowed(email):
                st.error("This email domain is not authorized for self-registration."); st.stop()

            existing = get_user_by_email(email)
            if existing and existing.get("is_verified"):
                st.warning("This email already has a verified account. Try logging in or reset your password."); st.stop()

            # Create or upsert the user (unverified)
            hashed_pw = Hasher.hash(pw1)
            insert_user(email=email, username=username, name=name, role="viewer", password_hash=hashed_pw)

            # Generate, hash, store + email the code
            code = _make_code(6)
            set_verification(email, _hash_code(code))
            send_email(
                to=email,
                subject="NP Analytics – Verify your email",
                body=(
                    f"Hi {name},\n\n"
                    f"Your verification code is: {code}\n"
                    f"This code expires in {os.getenv('VERIFICATION_MINUTES','15')} minutes.\n\n"
                    f"If you didn’t request this, you can ignore this email."
                ),
            )

            st.session_state["pending_verification_email"] = email
            st.success("Account created. Check your email for a 6-digit code, then verify below.")

def verification_panel():
    if st.session_state.get("authentication_status") is True:
        return
    email = st.session_state.get("pending_verification_email", "")
    with st.expander("✉️ Verify your email"):
        email = st.text_input("Email to verify", value=email).strip().lower()
        code  = st.text_input("6-digit code", max_chars=6)
        col1, col2 = st.columns(2)
        verify_btn = col1.button("Verify")
        resend_btn = col2.button("Resend code")

        if resend_btn:
            if not email:
                st.error("Enter your email above first.")
            else:
                new_code = _make_code(6)
                set_verification(email, _hash_code(new_code))
                # Optional: look up user's name for the greeting
                name = (get_user_by_email(email) or {}).get("name","there")
                send_email(
                    to=email,
                    subject="NP Analytics – Your new verification code",
                    body=(
                        f"Hi {name},\n\n"
                        f"Your verification code is: {new_code}\n"
                        f"This code expires in {os.getenv('VERIFICATION_MINUTES','15')} minutes."
                    ),
                )
                st.success("A new code was sent if the email exists.")

        if verify_btn:
            if not (email and code):
                st.error("Enter your email and code.")
            else:
                rec = get_verification(email)
                if not rec or not rec.get("hash"):
                    st.error("No verification in progress for that email.")
                else:
                    exp = rec.get("expires_at")
                    now = datetime.now(timezone.utc)
                    if exp and now > exp:
                        st.error("That code has expired. Click ‘Resend code’.")
                    elif _check_code(code, rec["hash"]):
                        mark_verified(email)
                        st.session_state.pop("pending_verification_email", None)
                        st.success("Email verified! You can now log in.")
                    else:
                        st.error("Invalid code. Check your email and try again.")

def password_tools():
    user = st.session_state.get("auth_user")
    if not user: return
    with st.expander("🔄 Change your password"):
        with st.form("pw_change"):
            pw1 = st.text_input("New password", type="password")
            pw2 = st.text_input("Confirm new password", type="password")
            go = st.form_submit_button("Update password")
        if go:
            if not pw1 or pw1 != pw2:
                st.error("Passwords must match."); st.stop()
            new_hash = Hasher.hash(pw1)
            update_password(user["email"], new_hash)
            st.success("Password updated.")
