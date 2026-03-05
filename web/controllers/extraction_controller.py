from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from flask_login import login_user, login_required, logout_user, current_user
from web.services.extraction_service import ExtractionService
from web.utils.security import USERS, User
import threading
from datetime import datetime, timedelta
from web.utils.etl_state import etl_state

# Blueprint
extraction_bp = Blueprint(
    "extraction",
    __name__,
    template_folder="../templates"
)

# Variables globales de ETL
cooldown_minutes = 5
last_execution = None
is_running = False


# ================= LOGIN =================
@extraction_bp.route("/login", methods=["GET", "POST"])
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
@extraction_bp.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("extraction.login"))


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
        ExtractionService.run_full_extraction()
    finally:
        is_running = False
        last_execution = datetime.now()


@extraction_bp.route("/run", methods=["POST"])
@login_required
def run():
    global is_running, last_execution

    now = datetime.now()

    if is_running:
        return jsonify({"status": "running"})

    if last_execution:
        if now < last_execution + timedelta(minutes=cooldown_minutes):
            remaining = (last_execution + timedelta(minutes=cooldown_minutes) - now).seconds
            return jsonify({"status": "cooldown", "remaining": remaining})

    is_running = True
    thread = threading.Thread(target=execute_job)
    thread.start()
    return jsonify({"status": "started"})


# ================= STATUS ENDPOINT =================
@extraction_bp.route("/status")
@login_required
def status():
    return jsonify({"is_running": is_running})

@extraction_bp.route("/progress")
@login_required
def progress():

    return jsonify(etl_state)
