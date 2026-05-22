import json
from pathlib import Path
from datetime import datetime

from flask import Blueprint, jsonify, render_template

from app.db.connection import get_db_connection

hbatch_handover_ui_bp = Blueprint("hbatch_handover_ui", __name__)
_WEB_ROOT = Path(__file__).resolve().parents[2]


def _split_csv(raw) -> list[str]:
    if raw is None:
        return []
    text = str(raw).strip()
    if not text:
        return []
    return [x.strip() for x in text.split(",") if x and x.strip()]


def _build_tubes_map_from_batch_json(raw_tubes_json) -> dict[tuple[int, int], list[str]]:
    out: dict[tuple[int, int], list[str]] = {}
    data = raw_tubes_json
    if isinstance(raw_tubes_json, str):
        try:
            data = json.loads(raw_tubes_json)
        except Exception:
            data = []
    if not isinstance(data, list):
        return out

    for row in data:
        if not isinstance(row, dict):
            continue
        try:
            bid = int(row.get("booking_id") or row.get("bookingId") or 0)
        except Exception:
            bid = 0
        try:
            pid = int(row.get("patient_id") or row.get("patientId") or 0)
        except Exception:
            pid = 0
        tube = str(
            row.get("tube_name")
            or row.get("tube")
            or row.get("specimen")
            or row.get("specimen_name")
            or ""
        ).strip()
        if bid <= 0 or pid <= 0 or not tube:
            continue
        key = (bid, pid)
        out.setdefault(key, [])
        if tube not in out[key]:
            out[key].append(tube)
    return out


def _files_from_hc_slip(booking_code: str, patient_code: str) -> list[dict]:
    if not booking_code or not patient_code:
        return []
    folder = _WEB_ROOT / "app" / "static" / "uploads" / "hc_slip" / booking_code / patient_code
    if not folder.exists():
        return []
    out = []
    for p in sorted(folder.glob("*")):
        if p.is_file():
            out.append({"name": p.name, "url": f"/static/uploads/hc_slip/{booking_code}/{patient_code}/{p.name}"})
    return out


@hbatch_handover_ui_bp.get("/hhome-collection/batch-handover-ui")
def batch_handover_ui_page():
    return render_template("hhome_collection/hbatch_handover_ui.html")


@hbatch_handover_ui_bp.get("/hhome-collection/batch-handover-ui-data")
def batch_handover_ui_data():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, batch_json, booking_ids, tubes_json, created_at, created_by
                FROM hhome_collection_batch
                ORDER BY id DESC
                LIMIT 50
                """
            )
            batch_rows = cur.fetchall() or []
            all_booking_ids: list[int] = []
            batch_booking_map: dict[int, list[int]] = {}
            batch_meta_map: dict[int, dict] = {}
            batch_tubes_map: dict[int, dict[tuple[int, int], list[str]]] = {}

            for br in batch_rows:
                batch_id = int(br.get("id") or 0)
                if batch_id <= 0:
                    continue
                raw_batch_json = br.get("batch_json")
                raw_booking_ids = br.get("booking_ids")
                meta = {}
                booking_ids = []
                try:
                    if isinstance(raw_batch_json, dict):
                        meta = raw_batch_json
                    elif raw_batch_json:
                        meta = json.loads(raw_batch_json)
                except Exception:
                    meta = {}
                try:
                    if isinstance(raw_booking_ids, list):
                        booking_ids = [int(x) for x in raw_booking_ids if int(x or 0) > 0]
                    elif raw_booking_ids:
                        booking_ids = [int(x) for x in (json.loads(raw_booking_ids) or []) if int(x or 0) > 0]
                except Exception:
                    booking_ids = []

                batch_booking_map[batch_id] = booking_ids
                batch_meta_map[batch_id] = {
                    "meta": meta,
                    "created_at": br.get("created_at"),
                    "created_by": br.get("created_by"),
                }
                batch_tubes_map[batch_id] = _build_tubes_map_from_batch_json(br.get("tubes_json"))
                all_booking_ids.extend(booking_ids)

            all_booking_ids = sorted(set(all_booking_ids))
            if not all_booking_ids:
                return jsonify({"ok": True, "dateIso": datetime.now().strftime("%Y-%m-%d"), "lastSync": datetime.now().strftime("%d %b %Y %I:%M %p"), "batches": []})

            placeholders_all = ",".join(["%s"] * len(all_booking_ids))
            cur.execute(
                f"""
                SELECT
                  hcb.id,
                  NULLIF(TRIM(hcb.booking_code), '') AS booking_code,
                  hcb.preferred_time_slot,
                  hcb.address_snapshot_json
                FROM hhome_collection_booking hcb
                WHERE hcb.id IN ({placeholders_all})
                ORDER BY hcb.id DESC
                LIMIT 100
                """
                ,
                all_booking_ids
            )
            bookings = cur.fetchall() or []

            booking_ids = [int(r["id"]) for r in bookings if r.get("id")]
            patients_by_booking = {bid: [] for bid in booking_ids}
            tests_by_booking_patient = {}

            if booking_ids:
                placeholders = ",".join(["%s"] * len(booking_ids))
                cur.execute(
                    f"""
                    SELECT
                      bp.booking_id,
                      bp.patient_id,
                      NULLIF(TRIM(hcb.booking_code), '') AS booking_code,
                      COALESCE(NULLIF(TRIM(p.patient_code), ''), CONCAT('PT', p.id)) AS patient_code,
                      COALESCE(NULLIF(TRIM(CONCAT_WS(' ', p.title, p.full_name)), ''), CONCAT('Patient ', p.id)) AS patient_name,
                      p.age_years,
                      p.gender,
                      COALESCE(NULLIF(TRIM(p.contact_mobile), ''), '') AS contact_mobile,
                      COALESCE(bp.prescription_files, '') AS prescription_files,
                      COALESCE(p.patient_documents, '') AS patient_documents
                    FROM hhome_collection_booking_patient bp
                    INNER JOIN hhome_collection_booking hcb ON hcb.id = bp.booking_id
                    INNER JOIN hpatient_master p ON p.id = bp.patient_id
                    WHERE bp.booking_id IN ({placeholders})
                    ORDER BY bp.booking_id, bp.id
                    """,
                    booking_ids,
                )
                for row in (cur.fetchall() or []):
                    bid = int(row.get("booking_id") or 0)
                    pid = int(row.get("patient_id") or 0)
                    if bid <= 0 or pid <= 0:
                        continue
                    booking_code = str(row.get("booking_code") or "").strip()
                    patient_code = str(row.get("patient_code") or "").strip()
                    prescriptions = [
                        {"name": x.split("/")[-1], "url": f"/static/uploads/prescriptions/{x}"}
                        for x in _split_csv(row.get("prescription_files"))
                    ]
                    patient_docs_raw = _split_csv(row.get("patient_documents"))
                    patient_photo = []
                    patient_docs = []
                    for n in patient_docs_raw:
                        url = f"/static/uploads/patient_documents/{n}"
                        if "_PHOTO_" in str(n).upper():
                            patient_photo.append({"name": n, "url": url})
                        else:
                            patient_docs.append({"name": n, "url": url})
                    trf_files = _files_from_hc_slip(booking_code, patient_code)
                    patients_by_booking.setdefault(bid, []).append(
                        {
                            "patientId": pid,
                            "patientCode": row.get("patient_code"),
                            "name": row.get("patient_name"),
                            "age": row.get("age_years"),
                            "gender": row.get("gender"),
                            "mobile": row.get("contact_mobile"),
                            "tests": [],
                            "tubes": [],
                            "docs": [
                                {"id": f"D-{bid}-{pid}-TRF", "type": "TRF / Lab Slip", "kind": "image", "files": trf_files},
                                {"id": f"D-{bid}-{pid}-PRESC", "type": "Prescription", "kind": "image", "files": prescriptions},
                                {"id": f"D-{bid}-{pid}-DOC", "type": "Patient Document", "kind": "image", "files": patient_docs},
                                {"id": f"D-{bid}-{pid}-PHOTO", "type": "Patient Photo", "kind": "image", "files": patient_photo},
                            ],
                        }
                    )

                cur.execute(
                    f"""
                    SELECT booking_id, patient_id, COALESCE(NULLIF(TRIM(test_name), ''), TRIM(booked_code)) AS test_name
                    FROM hhome_collection_booking_patient_test
                    WHERE booking_id IN ({placeholders}) AND IFNULL(test_status, 0) = 1
                    ORDER BY id
                    """,
                    booking_ids,
                )
                for row in (cur.fetchall() or []):
                    bid = int(row.get("booking_id") or 0)
                    pid = int(row.get("patient_id") or 0)
                    tname = str(row.get("test_name") or "").strip()
                    if bid <= 0 or pid <= 0 or not tname:
                        continue
                    key = (bid, pid)
                    tests_by_booking_patient.setdefault(key, [])
                    if tname not in tests_by_booking_patient[key]:
                        tests_by_booking_patient[key].append(tname)

            appts = []
            for r in bookings:
                bid = int(r.get("id") or 0)
                raw_snapshot = r.get("address_snapshot_json")
                snap = {}
                if isinstance(raw_snapshot, dict):
                    snap = raw_snapshot
                elif raw_snapshot:
                    try:
                        snap = json.loads(raw_snapshot)
                    except Exception:
                        snap = {}

                colony = str(snap.get("colony_name") or "-").strip() or "-"
                city = str(snap.get("city") or "-").strip() or "-"
                pin = str(snap.get("pincode") or "-").strip() or "-"
                slot = str(r.get("preferred_time_slot") or "-").strip() or "-"

                p_rows = patients_by_booking.get(bid, [])
                for p in p_rows:
                    key = (bid, int(p.get("patientId") or 0))
                    p["tests"] = tests_by_booking_patient.get(key, [])
                    p["tubes"] = []

                appts.append(
                    {
                        "bookingId": bid,
                        "rowType": "BOOKING",
                        "bookingCode": r.get("booking_code"),
                        "route": "",
                        "colony": f"{colony}, {city}, {pin}",
                        "slot": slot,
                        "patients": p_rows,
                    }
                )

            appt_map = {int(ap.get("bookingId") or 0): ap for ap in appts if int(ap.get("bookingId") or 0) > 0}

            batches = []
            for br in batch_rows:
                batch_id = int(br.get("id") or 0)
                if batch_id <= 0:
                    continue
                binfo = batch_meta_map.get(batch_id) or {}
                meta = binfo.get("meta") or {}
                raw_created_at = binfo.get("created_at")
                created_at_txt = raw_created_at.strftime("%I:%M %p") if hasattr(raw_created_at, "strftime") else datetime.now().strftime("%I:%M %p")
                created_date_txt = raw_created_at.strftime("%d-%m-%Y") if hasattr(raw_created_at, "strftime") else datetime.now().strftime("%d-%m-%Y")
                created_date_iso = raw_created_at.strftime("%Y-%m-%d") if hasattr(raw_created_at, "strftime") else datetime.now().strftime("%Y-%m-%d")
                booking_list = []
                for bid in (batch_booking_map.get(batch_id) or []):
                    item = appt_map.get(int(bid))
                    if item:
                        item = dict(item)
                        tubes_for_batch = batch_tubes_map.get(batch_id) or {}
                        for pp in (item.get("patients") or []):
                            pid = int(pp.get("patientId") or 0)
                            if pid > 0:
                                pp["tubes"] = tubes_for_batch.get((int(bid), pid), []) or []
                            pp.pop("patientId", None)
                        item.pop("bookingId", None)
                        booking_list.append(item)
                if not booking_list:
                    continue
                batches.append(
                    {
                        "batchId": str(meta.get("batch_id") or meta.get("batch_code") or f"HCBAT-{batch_id}"),
                        "status": "pending_verification",
                        "createdBy": str(meta.get("handover_to") or f"User {binfo.get('created_by') or '-'}"),
                        "phleboCode": str(meta.get("rider_name") or "-"),
                        "createdAt": created_at_txt,
                        "deviceId": created_date_txt,
                        "dateIso": created_date_iso,
                        "routeSummary": "Batch",
                        "appointments": booking_list,
                    }
                )

            payload = {
                "ok": True,
                "dateIso": datetime.now().strftime("%Y-%m-%d"),
                "lastSync": datetime.now().strftime("%d %b %Y %I:%M %p"),
                "batches": batches,
            }
            return jsonify(payload)
    finally:
        conn.close()
