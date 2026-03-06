from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from flask_login import login_user, login_required, logout_user, current_user
from web.services.extraction_service import ExtractionService
from web.utils.security import USERS, User
import threading
from datetime import datetime, timedelta
from web.utils.etl_state import etl_state

extraction_bp = Blueprint(
    "extraction",
    __name__,
    template_folder="../templates"
)

cooldown_minutes = 5
last_execution = None
is_running = False

# ================= DASHBOARD =================
@extraction_bp.route("/")
@login_required
def dashboard():
    return render_template("dashboard.html",
        user=current_user.id,
        is_running=is_running,
        last_execution=last_execution.strftime("%d-%m-%Y %H:%M:%S") if last_execution else None
    )

# ================= ETL JOB =================
def execute_job():
    global is_running, last_execution
    try:
        is_running = True
        etl_state["running"] = True
        etl_state["current_table"] = None
        etl_state["step"] = "starting"
        etl_state["progress"] = {}
        etl_state["percentage"] = 0

        ExtractionService.run_full_extraction()

    finally:
        is_running = False
        etl_state["running"] = False
        etl_state["current_table"] = None
        last_execution = datetime.now()

@extraction_bp.route("/run", methods=["POST"])
@login_required
def run():
    global is_running, last_execution
    now = datetime.now()
    if is_running:
        return jsonify({"status": "running"})
    if last_execution and now < last_execution + timedelta(minutes=cooldown_minutes):
        remaining = (last_execution + timedelta(minutes=cooldown_minutes) - now).seconds
        return jsonify({"status": "cooldown", "remaining": remaining})

    thread = threading.Thread(target=execute_job)
    thread.start()
    return jsonify({"status": "started"})

# ================= STATUS =================
@extraction_bp.route("/status")
@login_required
def status():
    now = datetime.now()
    cooldown_remaining = None
    if last_execution:
        cooldown_end = last_execution + timedelta(minutes=cooldown_minutes)
        if now < cooldown_end:
            cooldown_remaining = (cooldown_end - now).seconds
    return jsonify({
        "is_running": is_running,
        "last_execution": last_execution.strftime("%d-%m-%Y %H:%M:%S") if last_execution else None,
        "cooldown_remaining": cooldown_remaining
    })

# ================= PROGRESS =================
@extraction_bp.route("/progress")
@login_required
def progress():
    return jsonify({
        "running": etl_state.get("running", False),
        "table": etl_state.get("current_table"),
        "step": etl_state.get("step"),
        "progress": etl_state.get("percentage", 0)
    })

# ================= LOGIN =================
@extraction_bp.route("/login", methods=["GET", "POST"], endpoint="login")
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        if username in USERS and USERS[username] == password:
            login_user(User(username))
            return redirect(url_for("extraction.dashboard"))
        else:
            error = "Credenciales incorrectas"
    return render_template("login.html", error=error)

# ================= LOGOUT =================
@extraction_bp.route("/logout", endpoint="logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("extraction.login"))