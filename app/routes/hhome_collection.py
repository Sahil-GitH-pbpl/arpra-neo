from flask import Blueprint, jsonify, render_template, request, session
from datetime import date, timedelta

from app.home_collection_core import HHomeCollectionCore

hhome_collection_bp = Blueprint("hhome_collection", __name__)
service = HHomeCollectionCore()


@hhome_collection_bp.get("/hhome-collection")
def wizard():
    service.clear_booking_session(session)
    session.pop("search_mobile", None)
    return render_template("hhome_collection/hwizard.html")


@hhome_collection_bp.get("/hhome-collection/step/<int:step>")
def load_step(step: int):
    templates = {
        1: "hhome_collection/hstep1_caller.html",
        2: "hhome_collection/hstep2_patient.html",
        3: "hhome_collection/hstep3_address.html",
        4: "hhome_collection/hstep4_booking.html",
    }
    if step not in templates:
        return jsonify({"ok": False, "message": "Invalid step"}), 400
    return render_template(templates[step])


@hhome_collection_bp.post("/hhome-collection/search-caller")
def search_caller():
    payload = request.get_json(silent=True) or {}
    mobile = (payload.get("mobile") or "").strip()
    if not mobile:
        return jsonify({"ok": False, "message": "Mobile is required"}), 400

    caller = service.get_caller_by_mobile(mobile)
    if caller:
        service.reset_session_for_new_caller(session)
        session["hcaller_id"] = caller["id"]
        selected = session.get("hselected_patients", [])
        linked_patients = service.get_linked_patients(caller["id"], session)
        selected_enriched = service.get_selected_patients_enriched(caller["id"], selected)
        addresses = service.get_addresses_for_caller(caller["id"])
        return jsonify(
            {
                "ok": True,
                "found": True,
                "caller": caller,
                "linked_patients": linked_patients,
                "selected_patients": selected_enriched,
                "addresses": addresses,
                "selected_address_id": session.get("hselected_address_id"),
            }
        )

    service.reset_session_for_new_caller(session)
    session["search_mobile"] = mobile
    return jsonify(
        {
            "ok": True,
            "found": False,
            "mobile": mobile,
            "linked_patients": [],
            "selected_patients": [],
            "addresses": [],
            "selected_address_id": None,
        }
    )


@hhome_collection_bp.get("/hhome-collection/linked-patients")
def linked_patients():
    caller_id = session.get("hcaller_id")
    if not caller_id:
        return jsonify({"ok": True, "patients": []})
    return jsonify({"ok": True, "patients": service.get_linked_patients(caller_id, session)})


@hhome_collection_bp.get("/hhome-collection/current-caller")
def current_caller():
    caller_id = session.get("hcaller_id")
    if not caller_id:
        return jsonify({"ok": True, "caller": None})
    return jsonify({"ok": True, "caller": service.get_caller(caller_id)})


@hhome_collection_bp.post("/hhome-collection/select-patient")
def select_patient():
    caller_id = session.get("hcaller_id")
    if not caller_id:
        return jsonify({"ok": False, "message": "Caller is required first"}), 400

    payload = request.get_json(silent=True) or {}
    patient_id = payload.get("patient_id")
    result = service.select_patient(caller_id, patient_id, session, actor_user_id=session.get("user_id"))
    if result.get("ok"):
        result.update(service.get_step1_bundle(caller_id, session))
    status = 200 if result["ok"] else 400
    return jsonify(result), status


@hhome_collection_bp.post("/hhome-collection/remove-selected-patient")
def remove_selected_patient():
    caller_id = session.get("hcaller_id")
    payload = request.get_json(silent=True) or {}
    patient_id = int(payload.get("patient_id", 0))
    selected = session.get("hselected_patients", [])
    session["hselected_patients"] = [item for item in selected if int(item["patient_id"]) != patient_id]
    bundle = service.get_step1_bundle(caller_id, session)
    return jsonify({"ok": True, **bundle})


@hhome_collection_bp.get("/hhome-collection/selected-patients")
def selected_patients():
    caller_id = session.get("hcaller_id")
    selected = session.get("hselected_patients", [])
    enriched = service.get_selected_patients_enriched(caller_id, selected)
    return jsonify({"ok": True, "selected_patients": enriched})


@hhome_collection_bp.post("/hhome-collection/create-patient")
def create_patient():
    caller_id = session.get("hcaller_id")
    if caller_id and not service.get_caller(caller_id):
        session.pop("hcaller_id", None)
        caller_id = None

    payload = request.get_json(silent=True) or {}
    if not caller_id:
        contact_mobile = (payload.get("contact_mobile") or payload.get("searched_mobile") or "").strip()
        if not contact_mobile:
            return jsonify({"ok": False, "message": "Contact number is required for new caller"}), 400
        existing = service.get_caller_by_mobile(contact_mobile)
        if existing:
            caller_id = existing["id"]
        else:
            caller_payload = {
                "full_name": (payload.get("full_name") or "").strip(),
                "primary_mobile": contact_mobile,
                "alternate_mobile": (payload.get("alternate_mobile") or "").strip() or None,
                "email": (payload.get("email") or "").strip() or None,
            }
            created = service.create_caller(caller_payload, actor_user_id=session.get("user_id"))
            if not created["ok"]:
                return jsonify(created), 400
            caller_id = created["caller"]["id"]
        session["hcaller_id"] = caller_id

    result = service.create_patient_and_link(
        caller_id, payload, session, actor_user_id=session.get("user_id")
    )
    if result.get("ok"):
        result.update(service.get_step1_bundle(caller_id, session))
    status = 200 if result["ok"] else 400
    return jsonify(result), status


@hhome_collection_bp.get("/hhome-collection/patient/<int:patient_id>")
def patient_detail(patient_id: int):
    caller_id = session.get("hcaller_id")
    if not caller_id:
        return jsonify({"ok": False, "message": "Caller is required first"}), 400
    patient = service.get_patient_for_edit(caller_id, patient_id)
    if not patient:
        return jsonify({"ok": False, "message": "Patient not found"}), 404
    return jsonify({"ok": True, "patient": patient})


@hhome_collection_bp.patch("/hhome-collection/patient/<int:patient_id>")
def update_patient(patient_id: int):
    caller_id = session.get("hcaller_id")
    if not caller_id:
        return jsonify({"ok": False, "message": "Caller is required first"}), 400
    payload = request.get_json(silent=True) or {}
    result = service.update_patient_for_caller(
        caller_id, patient_id, payload, actor_user_id=session.get("user_id")
    )
    if result.get("ok"):
        result.update(service.get_step1_bundle(caller_id, session))
    status = 200 if result.get("ok") else 400
    return jsonify(result), status


@hhome_collection_bp.get("/hhome-collection/colonies")
def colonies():
    city = request.args.get("city")
    return jsonify({"ok": True, "colonies": service.list_colonies(city=city)})

@hhome_collection_bp.get("/hhome-collection/addresses")
def addresses():
    caller_id = session.get("hcaller_id")
    if not caller_id:
        return jsonify({"ok": True, "addresses": [], "selected_address_id": session.get("hselected_address_id")})
    return jsonify({
        "ok": True,
        "addresses": service.get_addresses_for_caller(caller_id),
        "selected_address_id": session.get("hselected_address_id"),
    })


@hhome_collection_bp.post("/hhome-collection/create-address")
def create_address():
    caller_id = session.get("hcaller_id")
    if not caller_id:
        return jsonify({"ok": False, "message": "Caller is required first"}), 400

    selected = session.get("hselected_patients", [])
    patient_ids = [int(x["patient_id"]) for x in selected]
    if not patient_ids:
        return jsonify({"ok": False, "message": "Select at least one patient first"}), 400

    payload = request.get_json(silent=True) or {}
    result = service.create_address_for_patients(
        patient_ids, payload, actor_user_id=session.get("user_id")
    )
    if result["ok"]:
        session["hselected_address_id"] = result["address"]["id"]
        session["hselected_address_snapshot"] = result["address_snapshot"]
    status = 200 if result["ok"] else 400
    return jsonify(result), status


@hhome_collection_bp.get("/hhome-collection/address/<int:address_id>")
def address_detail(address_id: int):
    caller_id = session.get("hcaller_id")
    if not caller_id:
        return jsonify({"ok": False, "message": "Caller is required first"}), 400
    address = service.get_address_for_caller(caller_id, address_id)
    if not address:
        return jsonify({"ok": False, "message": "Address not found"}), 404
    return jsonify({"ok": True, "address": address})


@hhome_collection_bp.patch("/hhome-collection/address/<int:address_id>")
def update_address(address_id: int):
    caller_id = session.get("hcaller_id")
    if not caller_id:
        return jsonify({"ok": False, "message": "Caller is required first"}), 400

    payload = request.get_json(silent=True) or {}
    result = service.update_address_for_caller(
        caller_id, address_id, payload, actor_user_id=session.get("user_id")
    )
    if result.get("ok"):
        session["hselected_address_id"] = result["address"]["id"]
        session["hselected_address_snapshot"] = result["address_snapshot"]
    status = 200 if result.get("ok") else 400
    return jsonify(result), status


@hhome_collection_bp.post("/hhome-collection/select-address")
def select_address():
    payload = request.get_json(silent=True) or {}
    address_id = int(payload.get("address_id", 0))
    snapshot = service.get_address_snapshot(address_id)
    if not snapshot:
        return jsonify({"ok": False, "message": "Address not found"}), 404

    session["hselected_address_id"] = address_id
    session["hselected_address_snapshot"] = snapshot
    return jsonify({"ok": True, "selected_address_id": address_id, "snapshot": snapshot})


@hhome_collection_bp.get("/hhome-collection/summary")
def summary():
    caller_id = session.get("hcaller_id")
    if not caller_id:
        return jsonify({"ok": False, "message": "Caller missing"}), 400

    caller = service.get_caller(caller_id)
    selected = session.get("hselected_patients", [])
    selected_enriched = service.get_selected_patients_enriched(caller_id, selected)
    address = session.get("hselected_address_snapshot")
    return jsonify({"ok": True, "caller": caller, "selected_patients": selected_enriched, "selected_address": address})


@hhome_collection_bp.get("/hhome-collection/route-slot-grid")
def route_slot_grid():
    visit_date = (request.args.get("date") or "").strip()
    if not visit_date:
        visit_date = (date.today() + timedelta(days=1)).isoformat()
    selected_route = (request.args.get("route") or "").strip()
    if not selected_route:
        selected_route = ((session.get("hselected_address_snapshot") or {}).get("route_no_snapshot") or "").strip()
    result = service.route_slot_grid_data(visit_date=visit_date, selected_route=selected_route)
    status = 200 if result.get("ok") else 400
    return jsonify(result), status


@hhome_collection_bp.get("/hhome-collection/internal-ref-users")
def internal_ref_users():
    try:
        q = (request.args.get("q") or "").strip()
        limit = request.args.get("limit", default=20, type=int)
        rows = service.search_active_staff_users(q=q, limit=limit)
        return jsonify({"ok": True, "items": rows})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)}), 500

@hhome_collection_bp.get("/hhome-collection/panel-companies")
def panel_companies():
    try:
        q = (request.args.get("q") or "").strip()
        atype = (request.args.get("atype") or "").strip()
        rows = service.search_panel_companies(
            q,
            limit=request.args.get("limit", default=15, type=int),
            atype=atype or None,
        )
        return jsonify({"ok": True, "items": rows})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)}), 500


@hhome_collection_bp.get("/hhome-collection/panel-groups")
def panel_groups():
    try:
        comp_cat_id = request.args.get("comp_cat_id")
        if not comp_cat_id:
            return jsonify({"ok": False, "message": "comp_cat_id is required"}), 400
        return jsonify({"ok": True, "groups": service.panel_groups(comp_cat_id)})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)}), 500


@hhome_collection_bp.get("/hhome-collection/panel-subgroups")
def panel_subgroups():
    try:
        comp_cat_id = request.args.get("comp_cat_id")
        gcode = request.args.get("gcode")
        if not comp_cat_id or not gcode:
            return jsonify({"ok": False, "message": "comp_cat_id and gcode are required"}), 400
        return jsonify({"ok": True, "subgroups": service.panel_subgroups(comp_cat_id, gcode)})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)}), 500


@hhome_collection_bp.get("/hhome-collection/panel-tests")
def panel_tests():
    try:
        comp_cat_id = request.args.get("comp_cat_id")
        gcode = request.args.get("gcode")
        scode = request.args.get("scode")
        if not comp_cat_id or not gcode or not scode:
            return jsonify({"ok": False, "message": "comp_cat_id, gcode and scode are required"}), 400
        return jsonify({"ok": True, "tests": service.panel_tests(comp_cat_id, gcode, scode)})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)}), 500


@hhome_collection_bp.get("/hhome-collection/panel-child-tests")
def panel_child_tests():
    try:
        parent_gcode = request.args.get("parent_gcode")
        parent_scode = request.args.get("parent_scode")
        parent_test_code = request.args.get("parent_test_code")
        if not parent_gcode or not parent_scode or not parent_test_code:
            return jsonify({"ok": False, "message": "parent_gcode, parent_scode and parent_test_code are required"}), 400
        rows = service.panel_child_tests(parent_gcode, parent_scode, parent_test_code)
        return jsonify({"ok": True, "tests": rows})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)}), 500


@hhome_collection_bp.get("/hhome-collection/test-specimen-catalog")
def test_specimen_catalog():
    try:
        return jsonify({"ok": True, **service.test_specimen_catalog()})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)}), 500


@hhome_collection_bp.post("/hhome-collection/confirm-booking")
def confirm_booking():
    caller_id = session.get("hcaller_id")
    selected_patients = session.get("hselected_patients", [])
    selected_address_id = session.get("hselected_address_id")
    selected_snapshot = session.get("hselected_address_snapshot")

    payload = request.get_json(silent=True) or {}
    result = service.confirm_booking(
        caller_id=caller_id,
        selected_patients=selected_patients,
        selected_address_id=selected_address_id,
        selected_snapshot=selected_snapshot,
        payload=payload,
        actor_user_id=session.get("user_id"),
    )

    if result["ok"]:
        session["last_booking_id"] = result["booking_id"]
        service.clear_booking_session(session)
    status = 200 if result["ok"] else 400
    return jsonify(result), status


@hhome_collection_bp.get("/hhome-collection/success")
def success():
    booking_id = request.args.get("booking_id", type=int)
    if not booking_id:
        return render_template("hhome_collection/hsuccess.html", booking=None)
    return render_template("hhome_collection/hsuccess.html", booking=service.get_booking_full(booking_id))


@hhome_collection_bp.get("/hhome-collection/print/<int:booking_id>")
def print_slip(booking_id: int):
    booking = service.get_booking_full(booking_id)
    if not booking:
        return "Booking not found", 404
    return render_template("hhome_collection/hprint_slip.html", booking=booking)




