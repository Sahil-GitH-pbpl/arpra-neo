from flask import Blueprint, jsonify, render_template, request, session
from datetime import date, timedelta

from app.home_collection_core import HHomeCollectionCore

hhome_collection_dashboard_bp = Blueprint("hhome_collection_dashboard", __name__)
service = HHomeCollectionCore()


@hhome_collection_dashboard_bp.get("/hhome-collection/dashboard")
def dashboard():
    return render_template("hhome_collection/hdashboard.html")


@hhome_collection_dashboard_bp.get("/hhome-collection/assign-booking")
def assign_booking():
    default_date = (date.today() + timedelta(days=1)).isoformat()
    return render_template("hhome_collection/hassign_booking.html", default_date=default_date)


@hhome_collection_dashboard_bp.get("/hhome-collection/dashboard-data")
def dashboard_data():
    params = {
        "date_from": request.args.get("date_from"),
        "date_to": request.args.get("date_to"),
        "status": request.args.get("status"),
        "route": request.args.get("route"),
        "search": request.args.get("search"),
    }
    return jsonify({"ok": True, "rows": service.dashboard_rows(params)})


@hhome_collection_dashboard_bp.get("/hhome-collection/assign-booking-data")
def assign_booking_data():
    plan_date = request.args.get("date")
    return jsonify(service.assignment_planner_data(plan_date))


@hhome_collection_dashboard_bp.post("/hhome-collection/assign-bookings-commit")
def assign_bookings_commit():
    payload = request.get_json(silent=True) or {}
    result = service.commit_assignment_plan(
        plan_date=payload.get("plan_date"),
        assignments=payload.get("assignments") or [],
        actor_user_id=session.get("user_id"),
    )
    status = 200 if result.get("ok") else 400
    return jsonify(result), status


@hhome_collection_dashboard_bp.get("/hhome-collection/booking/<int:booking_id>")
def booking_detail(booking_id: int):
    booking = service.get_booking_full(booking_id)
    if not booking:
        return jsonify({"ok": False, "message": "Not found"}), 404
    return jsonify({"ok": True, "booking": booking})


@hhome_collection_dashboard_bp.get("/hhome-collection/phlebotomists")
def phlebotomists():
    return jsonify({"ok": True, "phlebotomists": service.get_phlebotomists()})


@hhome_collection_dashboard_bp.post("/hhome-collection/assign-phlebotomist")
def assign_phlebotomist():
    payload = request.get_json(silent=True) or {}
    booking_id = int(payload.get("booking_id", 0))
    appointment_id = int(payload.get("appointment_id", 0))
    user_id = int(payload.get("user_id", 0))
    result = service.assign_phlebotomist(
        booking_id,
        user_id,
        actor_user_id=session.get("user_id"),
        appointment_id=appointment_id,
    )
    status = 200 if result["ok"] else 400
    return jsonify(result), status


@hhome_collection_dashboard_bp.post("/hhome-collection/cancel-booking")
def cancel_booking():
    payload = request.get_json(silent=True) or {}
    booking_id = int(payload.get("booking_id", 0))
    appointment_id = int(payload.get("appointment_id", 0))
    reason_text = (payload.get("reason_text") or "").strip()
    result = service.cancel_booking(
        booking_id,
        reason_text=reason_text,
        actor_user_id=session.get("user_id"),
        appointment_id=appointment_id,
    )
    status = 200 if result["ok"] else 400
    return jsonify(result), status


@hhome_collection_dashboard_bp.post("/hhome-collection/book-appointment-init")
def book_appointment_init():
    payload = request.get_json(silent=True) or {}
    booking_id = int(payload.get("booking_id", 0))
    reason_text = (payload.get("reason_text") or "").strip()
    result = service.begin_followup_appointment_session(
        booking_id=booking_id,
        reason_text=reason_text,
        session=session,
        actor_user_id=session.get("user_id"),
    )
    status = 200 if result.get("ok") else 400
    return jsonify(result), status


@hhome_collection_dashboard_bp.post("/hhome-collection/modify-init")
def modify_init():
    payload = request.get_json(silent=True) or {}
    booking_id = int(payload.get("booking_id", 0))
    appointment_id = int(payload.get("appointment_id", 0))
    reason_text = (payload.get("reason_text") or "").strip()
    if appointment_id > 0:
        result = service.begin_modify_appointment_session(
            booking_id=booking_id,
            appointment_id=appointment_id,
            reason_text=reason_text,
            session=session,
            actor_user_id=session.get("user_id"),
        )
    else:
        result = service.begin_modify_booking_session(
            booking_id=booking_id,
            reason_text=reason_text,
            session=session,
            actor_user_id=session.get("user_id"),
        )
    status = 200 if result.get("ok") else 400
    return jsonify(result), status
