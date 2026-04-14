import uuid
from datetime import date
import re
from threading import Lock

from app.db.connection import get_bhasin7001_connection, get_db_connection
from app.home_collection_core.hcodegen import hage_label, hcalculate_age_parts, hcode_from_id, hto_json

HSYSTEM_USER_ID = 1
HALLOWED_PATIENT_TAGS = ["VIP", "High Value", "Urgent"]


class HHomeCollectionCore:
    def __init__(self):
        self._panel_lock = Lock()
        self._panel_loaded = False
        self._panel_catalog = {
            "panels": [],
            "prefix2": {},
            "groups_by_comp": {},
            "subgroups_by_comp_g": {},
            "tests_by_comp_g_s": {},
            "profile_children_map": {},
            "test_by_testcode1": {},
            "test_by_g_s_testcode": {},
        }

    def _norm_code(self, v) -> str:
        if v is None:
            return ""
        return str(v).replace("\x00", "").strip()

    def _normalize_charge_mode(self, v) -> str:
        raw = self._norm_code(v).upper()
        if not raw:
            return ""
        keep = []
        for ch in raw:
            if ch in ("C", "P", "F") and ch not in keep:
                keep.append(ch)
        ordered = [x for x in ("C", "P", "F") if x in keep]
        return "".join(ordered)

    def _normalize_wa_target(self, phone: str) -> str:
        digits = re.sub(r"\D", "", str(phone or ""))
        if not digits:
            return ""
        if len(digits) == 10:
            return "91" + digits
        if len(digits) == 11 and digits.startswith("0"):
            return "91" + digits[1:]
        if len(digits) == 12 and digits.startswith("91"):
            return digits
        return digits

    def preload_panel_catalog(self):
        """Load panel/company + billing + GST tree once per server start."""
        with self._panel_lock:
            if self._panel_loaded:
                return

            conn = get_bhasin7001_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT
                            a.CenterID,
                            a.pname,
                            a.category AS CompCatID,
                            cc.CatDetails,
                            a.BillingChargeMode,
                            a.Atype
                        FROM address a
                        LEFT JOIN compcategory cc ON cc.CompCatID = a.category
                        WHERE a.pname IS NOT NULL
                          AND TRIM(a.pname) <> ''
                          AND UPPER(TRIM(a.Atype)) IN ('C','D')
                        ORDER BY a.pname
                        """
                    )
                    panel_rows = cur.fetchall()

                    cur.execute("SELECT Gcode, Description FROM groupmaster")
                    group_name = {self._norm_code(r["Gcode"]): self._norm_code(r.get("Description")) for r in cur.fetchall()}

                    cur.execute("SELECT Gcode, Scode, Description FROM subgroup")
                    subgroup_name = {
                        (self._norm_code(r["Gcode"]), self._norm_code(r["Scode"])): self._norm_code(r.get("Description"))
                        for r in cur.fetchall()
                    }

                    cur.execute(
                        """
                        SELECT Gcode, Scode, TestCode, Testcode1, Description, Profile, SpecimenID
                        FROM test
                        """
                    )
                    test_rows = cur.fetchall()
                    cur.execute(
                        """
                        SELECT SpecimenID, SpName
                        FROM testspecimen
                        """
                    )
                    specimen_name_by_id = {}
                    for sr in cur.fetchall():
                        sid = sr.get("SpecimenID")
                        if sid is None:
                            continue
                        try:
                            specimen_name_by_id[int(sid)] = self._norm_code(sr.get("SpName"))
                        except Exception:
                            continue

                    test_by_gst = {}
                    test_by_code1 = {}
                    for r in test_rows:
                        g = self._norm_code(r.get("Gcode"))
                        s = self._norm_code(r.get("Scode"))
                        tc = self._norm_code(r.get("TestCode"))
                        tc1 = self._norm_code(r.get("Testcode1"))
                        desc = self._norm_code(r.get("Description"))
                        is_profile = int(r.get("Profile") or 0) == 1
                        try:
                            specimen_id = int(r.get("SpecimenID")) if r.get("SpecimenID") is not None else None
                        except Exception:
                            specimen_id = None
                        base = {
                            "gcode": g,
                            "scode": s,
                            "test_code": tc,
                            "testcode1": tc1,
                            "description": desc,
                            "is_profile": is_profile,
                            "specimen_id": specimen_id,
                            "specimen_name": specimen_name_by_id.get(specimen_id, ""),
                        }
                        test_by_gst[(g, s, tc)] = base
                        if tc1:
                            test_by_code1[tc1] = base

                    cur.execute(
                        """
                        SELECT Gcode, SCode, ProfileCode, TestCode
                        FROM testprofile
                        """
                    )
                    testprofile_rows = cur.fetchall()

                    cur.execute(
                        """
                        SELECT
                            CompCatID, GCode, SCode, TestCode, CTestCode, CTestName,
                            Charge, MRP, MaxDiscount
                        FROM panelrates
                        WHERE BookedFlag = 1
                        """
                    )
                    rate_rows = cur.fetchall()
            finally:
                conn.close()

            panels = []
            prefix2 = {}
            panel_map = {}
            for r in panel_rows:
                pname = self._norm_code(r.get("pname"))
                if not pname:
                    continue
                comp_cat = self._norm_code(r.get("CompCatID"))
                key = (pname.lower(), comp_cat)
                mode = self._normalize_charge_mode(r.get("BillingChargeMode"))
                if key not in panel_map:
                    atype = self._norm_code(r.get("Atype")).upper()
                    panel = {
                        "CenterID": r.get("CenterID"),
                        "pname": pname,
                        "CompCatID": r.get("CompCatID"),
                        "CatDetails": self._norm_code(r.get("CatDetails")),
                        "BillingChargeMode": mode,
                        "_has_c": atype == "C",
                        "_has_d": atype == "D",
                        "_pname_lc": pname.lower(),
                    }
                    panel_map[key] = panel
                    panels.append(panel)
                    p2 = panel["_pname_lc"][:2]
                    prefix2.setdefault(p2, []).append(panel)
                else:
                    atype = self._norm_code(r.get("Atype")).upper()
                    if atype == "C":
                        panel_map[key]["_has_c"] = True
                    elif atype == "D":
                        panel_map[key]["_has_d"] = True
                    if mode:
                        existing = panel_map[key].get("BillingChargeMode") or ""
                        merged = self._normalize_charge_mode(existing + mode)
                        panel_map[key]["BillingChargeMode"] = merged

            groups_by_comp = {}
            subgroups_by_comp_g = {}
            tests_by_comp_g_s = {}
            profile_children_map = {}
            seen_tests = set()

            for r in rate_rows:
                cc = self._norm_code(r.get("CompCatID"))
                g = self._norm_code(r.get("GCode"))
                s = self._norm_code(r.get("SCode"))
                if not cc or not g:
                    continue

                gname = group_name.get(g, "")
                groups_by_comp.setdefault(cc, {})
                groups_by_comp[cc][g] = {"gcode": g, "description": gname}

                if s:
                    sname = subgroup_name.get((g, s), "")
                    subgroups_by_comp_g.setdefault((cc, g), {})
                    subgroups_by_comp_g[(cc, g)][s] = {"scode": s, "description": sname}

                panel_test_code = self._norm_code(r.get("TestCode"))
                panel_ctest_code = self._norm_code(r.get("CTestCode"))
                panel_ctest_name = self._norm_code(r.get("CTestName"))

                meta = test_by_gst.get((g, s, panel_test_code)) if s and panel_test_code else None
                if not meta and panel_ctest_code:
                    meta = test_by_code1.get(panel_ctest_code)

                test_code = self._norm_code((meta or {}).get("test_code")) or panel_test_code
                testcode1 = self._norm_code((meta or {}).get("testcode1")) or panel_ctest_code
                description = self._norm_code((meta or {}).get("description")) or panel_ctest_name
                booked_code = testcode1 or test_code
                if not booked_code:
                    continue

                t_key = (cc, g, s, booked_code)
                if t_key in seen_tests:
                    continue
                seen_tests.add(t_key)

                tests_by_comp_g_s.setdefault((cc, g, s), []).append(
                    {
                        "gcode": g,
                        "scode": s,
                        "test_code": test_code,
                        "testcode1": testcode1,
                        "booked_code": booked_code,
                        "description": description,
                        "charge": r.get("Charge"),
                        "mrp": r.get("MRP"),
                        "max_discount": r.get("MaxDiscount"),
                        "is_profile": bool((meta or {}).get("is_profile")),
                    }
                )

            for r in testprofile_rows:
                g = self._norm_code(r.get("Gcode"))
                s = self._norm_code(r.get("SCode"))
                profile_code = self._norm_code(r.get("ProfileCode"))
                child_testcode1 = self._norm_code(r.get("TestCode"))
                if not g or not s or not profile_code or not child_testcode1:
                    continue
                child = test_by_code1.get(child_testcode1)
                if not child:
                    continue
                key = (g, s, profile_code)
                profile_children_map.setdefault(key, [])
                profile_children_map[key].append(
                    {
                        "gcode": child.get("gcode") or g,
                        "scode": child.get("scode") or s,
                        "test_code": child.get("test_code") or "",
                        "testcode1": child.get("testcode1") or child_testcode1,
                        "booked_code": child.get("testcode1") or child.get("test_code") or child_testcode1,
                        "description": child.get("description") or "",
                        "is_profile": bool(child.get("is_profile")),
                    }
                )

            for key, rows in profile_children_map.items():
                uniq = {}
                for x in rows:
                    ukey = (
                        self._norm_code(x.get("gcode")),
                        self._norm_code(x.get("scode")),
                        self._norm_code(x.get("test_code")),
                        self._norm_code(x.get("testcode1")),
                    )
                    uniq[ukey] = x
                profile_children_map[key] = sorted(
                    uniq.values(),
                    key=lambda x: (x.get("test_code") or "", x.get("testcode1") or "", x.get("booked_code") or ""),
                )

            for key, arr in tests_by_comp_g_s.items():
                for item in arr:
                    parent_key = (
                        self._norm_code(item.get("gcode")),
                        self._norm_code(item.get("scode")),
                        self._norm_code(item.get("test_code")),
                    )
                    item["has_children"] = bool(profile_children_map.get(parent_key))
                arr.sort(
                    key=lambda x: (
                        x.get("test_code") or "",
                        x.get("testcode1") or "",
                        x.get("booked_code") or "",
                    )
                )

            self._panel_catalog = {
                "panels": panels,
                "prefix2": prefix2,
                "groups_by_comp": groups_by_comp,
                "subgroups_by_comp_g": subgroups_by_comp_g,
                "tests_by_comp_g_s": tests_by_comp_g_s,
                "profile_children_map": profile_children_map,
                "test_by_testcode1": test_by_code1,
                "test_by_g_s_testcode": test_by_gst,
            }
            self._panel_loaded = True

    def _actor(self, actor_user_id=None) -> int:
        try:
            uid = int(actor_user_id or 0)
            if uid > 0:
                return uid
        except Exception:
            pass
        return HSYSTEM_USER_ID

    def sanitize_patient_tags(self, raw_tag: str) -> str:
        items = [x.strip() for x in (raw_tag or "").split(",") if x.strip()]
        seen = set()
        filtered = []
        for allowed in HALLOWED_PATIENT_TAGS:
            if allowed in items and allowed not in seen:
                seen.add(allowed)
                filtered.append(allowed)
        return ",".join(filtered)

    def normalize_mobile(self, mobile: str) -> str:
        digits = re.sub(r"\D", "", (mobile or "").strip())
        if not digits:
            return ""
        if len(digits) > 10:
            digits = digits[-10:]
        return digits

    def reset_session_for_new_caller(self, session):
        session.pop("hcaller_id", None)
        session["hselected_patients"] = []
        session["hselected_address_id"] = None
        session["hselected_address_snapshot"] = None

    def clear_booking_session(self, session):
        session.pop("hcaller_id", None)
        session.pop("hselected_patients", None)
        session.pop("hselected_address_id", None)
        session.pop("hselected_address_snapshot", None)

    def _temp_code(self, prefix: str) -> str:
        return f"{prefix}{uuid.uuid4().hex[:12]}"

    def get_caller_by_mobile(self, mobile: str):
        mobile_norm = self.normalize_mobile(mobile)
        if not mobile_norm:
            return None

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, caller_code, full_name, primary_mobile, alternate_mobile,
                           email, caller_status
                    FROM hcaller_master
                    WHERE id = (
                        SELECT caller_id
                        FROM hcaller_mobile_map
                        WHERE mobile_norm = %s AND is_active = 1
                        LIMIT 1
                    )
                    LIMIT 1
                    """,
                    (mobile_norm,),
                )
                caller = cur.fetchone()
                if caller:
                    return caller

                cur.execute(
                    """
                    SELECT id, caller_code, full_name, primary_mobile, alternate_mobile,
                           email, caller_status
                    FROM hcaller_master
                    WHERE primary_mobile = %s OR alternate_mobile = %s
                    LIMIT 1
                    """,
                    (mobile_norm, mobile_norm),
                )
                caller = cur.fetchone()
                if caller:
                    self._upsert_caller_mobile(
                        cur,
                        caller["id"],
                        caller.get("primary_mobile"),
                        "Primary",
                        raise_on_conflict=False,
                    )
                    self._upsert_caller_mobile(
                        cur,
                        caller["id"],
                        caller.get("alternate_mobile"),
                        "Alternate",
                        raise_on_conflict=False,
                    )
                    conn.commit()
                return caller
        finally:
            conn.close()

    def get_caller(self, caller_id: int):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, caller_code, full_name, primary_mobile FROM hcaller_master WHERE id=%s",
                    (caller_id,),
                )
                return cur.fetchone()
        finally:
            conn.close()

    def _upsert_caller_mobile(
        self,
        cur,
        caller_id: int,
        mobile: str,
        phone_type: str,
        raise_on_conflict=True,
        actor_user_id=None,
    ):
        mobile_norm = self.normalize_mobile(mobile)
        if not mobile_norm:
            return

        cur.execute(
            """
            SELECT caller_id
            FROM hcaller_mobile_map
            WHERE mobile_norm = %s AND is_active = 1
            LIMIT 1
            """,
            (mobile_norm,),
        )
        existing = cur.fetchone()
        if existing and int(existing["caller_id"]) != int(caller_id):
            if raise_on_conflict:
                raise ValueError(f"Mobile {mobile_norm} is already mapped to another caller")
            return
        raw = (mobile or "").strip()

        cur.execute(
            """
            INSERT INTO hcaller_mobile_map
            (caller_id, mobile_norm, mobile_raw, phone_type, is_active, created_by)
            VALUES (%s,%s,%s,%s,1,%s)
            ON DUPLICATE KEY UPDATE
            caller_id = VALUES(caller_id),
            mobile_raw = VALUES(mobile_raw),
            phone_type = VALUES(phone_type),
            is_active = 1
            """,
            (caller_id, mobile_norm, raw or mobile_norm, phone_type, self._actor(actor_user_id)),
        )

    def create_caller(self, payload: dict, actor_user_id=None):
        actor = self._actor(actor_user_id)
        full_name = (payload.get("full_name") or "").strip()
        primary_mobile = self.normalize_mobile(payload.get("primary_mobile"))
        alternate_mobile = self.normalize_mobile(payload.get("alternate_mobile"))
        if not full_name or not primary_mobile:
            return {"ok": False, "message": "Full name and primary mobile are required"}

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT caller_id FROM hcaller_mobile_map WHERE mobile_norm=%s AND is_active=1 LIMIT 1",
                    (primary_mobile,),
                )
                if cur.fetchone():
                    return {"ok": False, "message": "This mobile is already registered"}

                temp_code = self._temp_code("HCLR-TMP-")
                cur.execute(
                    """
                    INSERT INTO hcaller_master
                    (caller_code, full_name, primary_mobile, alternate_mobile, email,
                     caller_status, created_by, updated_by)
                    VALUES (%s,%s,%s,%s,%s,'Active',%s,%s)
                    """,
                    (
                        temp_code,
                        full_name,
                        primary_mobile,
                        alternate_mobile or None,
                        payload.get("email"),
                        actor,
                        actor,
                    ),
                )
                caller_id = cur.lastrowid
                caller_code = hcode_from_id("HCLR-", caller_id)
                cur.execute("UPDATE hcaller_master SET caller_code=%s WHERE id=%s", (caller_code, caller_id))
                self._upsert_caller_mobile(cur, caller_id, primary_mobile, "Primary", actor_user_id=actor)
                self._upsert_caller_mobile(cur, caller_id, alternate_mobile, "Alternate", actor_user_id=actor)
                conn.commit()

                return {
                    "ok": True,
                    "caller": {
                        "id": caller_id,
                        "caller_code": caller_code,
                        "full_name": full_name,
                        "primary_mobile": primary_mobile,
                    },
                }
        except Exception as exc:
            conn.rollback()
            return {"ok": False, "message": str(exc)}
        finally:
            conn.close()

    def get_linked_patients(self, caller_id: int, session):
        conn = get_db_connection()
        try:
            selected = {int(x["patient_id"]) for x in session.get("hselected_patients", [])}
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.id, p.patient_code, CONCAT_WS(' ', p.title, p.full_name) AS full_name, p.tag, p.age_years, p.date_of_birth,
                           am.id AS default_address_id,
                           CONCAT_WS(', ', am.house_flat_no, am.floor, am.colony_name_snapshot, am.pincode_snapshot) AS default_address
                    FROM hcaller_patient_link cpl
                    INNER JOIN hpatient_master p ON p.id = cpl.patient_id
                    LEFT JOIN hpatient_address_link pal ON pal.id = (
                        SELECT pal2.id
                        FROM hpatient_address_link pal2
                        WHERE pal2.patient_id = p.id AND pal2.is_default = 1 AND pal2.is_active = 1
                        ORDER BY pal2.id DESC
                        LIMIT 1
                    )
                    LEFT JOIN haddress_master am ON am.id = pal.address_id
                    WHERE cpl.caller_id = %s AND cpl.is_active = 1
                    ORDER BY p.full_name
                    """,
                    (caller_id,),
                )
                rows = cur.fetchall()
                for row in rows:
                    row["age"] = hage_label(row.get("age_years"), row.get("date_of_birth"))
                    row["selected"] = int(row["id"]) in selected
                return rows
        finally:
            conn.close()

    def _upsert_caller_patient_link(self, cur, caller_id: int, patient_id: int, actor_user_id=None):
        cur.execute(
            """
            INSERT INTO hcaller_patient_link
            (caller_id, patient_id, is_active, created_by)
            VALUES (%s,%s,1,%s)
            ON DUPLICATE KEY UPDATE
            is_active = 1
            """,
            (caller_id, patient_id, self._actor(actor_user_id)),
        )

    def select_patient(self, caller_id: int, patient_id, session, actor_user_id=None):
        try:
            patient_id = int(patient_id)
        except Exception:
            return {"ok": False, "message": "Invalid patient"}

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, full_name FROM hpatient_master WHERE id=%s", (patient_id,))
                patient = cur.fetchone()
                if not patient:
                    return {"ok": False, "message": "Patient not found"}

                self._upsert_caller_patient_link(cur, caller_id, patient_id, actor_user_id=actor_user_id)
                conn.commit()

            selected = session.get("hselected_patients", [])
            exists = any(int(x["patient_id"]) == patient_id for x in selected)
            if not exists:
                selected.append({"patient_id": patient_id})
            session["hselected_patients"] = selected
            return {"ok": True, "selected_patients": selected}
        except Exception as exc:
            conn.rollback()
            return {"ok": False, "message": str(exc)}
        finally:
            conn.close()

    def create_patient_and_link(self, caller_id: int, payload: dict, session, actor_user_id=None):
        actor = self._actor(actor_user_id)
        full_name_input = (payload.get("full_name") or "").strip()
        title = (payload.get("title") or "").strip() or None
        labmate_pid = (payload.get("labmate_pid") or "").strip() or None
        panel_company = (payload.get("panel_company") or "").strip() or None
        gender = (payload.get("gender") or "").strip()
        if not full_name_input or not gender:
            return {"ok": False, "message": "Patient full name and gender are required"}
        full_name = full_name_input
        tag = self.sanitize_patient_tags(payload.get("tag"))
        dob = payload.get("date_of_birth") or None
        age_years = payload.get("age_years")
        contact_mobile = self.normalize_mobile(payload.get("contact_mobile"))
        alternate_mobile = self.normalize_mobile(payload.get("alternate_mobile"))

        if dob and not age_years:
            age_years, _ = hcalculate_age_parts(dob)

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                self._upsert_caller_mobile(
                    cur, caller_id, payload.get("contact_mobile"), "PatientContact", actor_user_id=actor
                )
                self._upsert_caller_mobile(
                    cur, caller_id, payload.get("alternate_mobile"), "Alternate", actor_user_id=actor
                )
                temp_code = self._temp_code("HPT-TMP-")
                cur.execute(
                    """
                    INSERT INTO hpatient_master
                    (patient_code, title, full_name, labmate_pid, panel_company, tag,
                     gender, date_of_birth, age_years, contact_mobile, alternate_mobile,
                     patient_status, created_by, updated_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Active',%s,%s)
                    """,
                    (
                        temp_code,
                        title,
                        full_name,
                        labmate_pid,
                        panel_company,
                        tag or None,
                        gender,
                        dob,
                        age_years,
                        contact_mobile or None,
                        alternate_mobile or None,
                        actor,
                        actor,
                    ),
                )
                patient_id = cur.lastrowid
                patient_code = hcode_from_id("HPT-HC-", patient_id)
                cur.execute("UPDATE hpatient_master SET patient_code=%s WHERE id=%s", (patient_code, patient_id))

                self._upsert_caller_patient_link(cur, caller_id, patient_id, actor_user_id=actor)
                conn.commit()

            selected = session.get("hselected_patients", [])
            selected.append({"patient_id": patient_id})
            session["hselected_patients"] = selected

            return {
                "ok": True,
                "patient": {
                    "id": patient_id,
                    "patient_code": patient_code,
                    "full_name": full_name,
                },
                "selected_patients": selected,
            }
        except Exception as exc:
            conn.rollback()
            return {"ok": False, "message": str(exc)}
        finally:
            conn.close()

    def get_patient_for_edit(self, caller_id: int, patient_id: int):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.id, p.title, p.full_name, p.labmate_pid, p.panel_company, p.tag,
                           p.gender, p.date_of_birth, p.age_years, p.contact_mobile, p.alternate_mobile,
                           cm.email
                    FROM hcaller_patient_link cpl
                    INNER JOIN hpatient_master p ON p.id = cpl.patient_id
                    INNER JOIN hcaller_master cm ON cm.id = cpl.caller_id
                    WHERE cpl.caller_id = %s
                      AND p.id = %s
                      AND cpl.is_active = 1
                    LIMIT 1
                    """,
                    (caller_id, patient_id),
                )
                row = cur.fetchone()
                if not row:
                    return None
                if row.get("date_of_birth"):
                    row["date_of_birth"] = row["date_of_birth"].isoformat()
                return row
        finally:
            conn.close()

    def update_patient_for_caller(self, caller_id: int, patient_id: int, payload: dict, actor_user_id=None):
        actor = self._actor(actor_user_id)
        full_name = (payload.get("full_name") or "").strip()
        gender = (payload.get("gender") or "").strip()
        if not full_name or not gender:
            return {"ok": False, "message": "Patient full name and gender are required"}

        title = (payload.get("title") or "").strip() or None
        labmate_pid = (payload.get("labmate_pid") or "").strip() or None
        panel_company = (payload.get("panel_company") or "").strip() or None
        tag = self.sanitize_patient_tags(payload.get("tag"))
        dob = payload.get("date_of_birth") or None
        age_years = payload.get("age_years")
        if dob and not age_years:
            age_years, _ = hcalculate_age_parts(dob)

        contact_mobile = self.normalize_mobile(payload.get("contact_mobile"))
        alternate_mobile = self.normalize_mobile(payload.get("alternate_mobile"))
        email = (payload.get("email") or "").strip() or None

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM hcaller_patient_link
                    WHERE caller_id = %s AND patient_id = %s AND is_active = 1
                    LIMIT 1
                    """,
                    (caller_id, patient_id),
                )
                if not cur.fetchone():
                    return {"ok": False, "message": "Patient not linked with selected caller"}

                cur.execute(
                    """
                    UPDATE hpatient_master
                    SET title=%s,
                        full_name=%s,
                        labmate_pid=%s,
                        panel_company=%s,
                        tag=%s,
                        gender=%s,
                        date_of_birth=%s,
                        age_years=%s,
                        contact_mobile=%s,
                        alternate_mobile=%s,
                        updated_by=%s
                    WHERE id=%s
                    """,
                    (
                        title,
                        full_name,
                        labmate_pid,
                        panel_company,
                        tag or None,
                        gender,
                        dob,
                        age_years,
                        contact_mobile or None,
                        alternate_mobile or None,
                        actor,
                        patient_id,
                    ),
                )

                # Keep caller contacts searchable in map table.
                self._upsert_caller_mobile(
                    cur, caller_id, contact_mobile, "PatientContact", actor_user_id=actor
                )
                self._upsert_caller_mobile(
                    cur, caller_id, alternate_mobile, "Alternate", actor_user_id=actor
                )
                if email:
                    cur.execute(
                        "UPDATE hcaller_master SET email=%s, updated_by=%s WHERE id=%s",
                        (email, actor, caller_id),
                    )

                conn.commit()
                return {"ok": True}
        except Exception as exc:
            conn.rollback()
            return {"ok": False, "message": str(exc)}
        finally:
            conn.close()

    def get_selected_patients_enriched(self, caller_id: int, selected: list):
        patient_ids = [int(item["patient_id"]) for item in selected]
        if not patient_ids:
            return []

        conn = get_db_connection()
        try:
            placeholders = ",".join(["%s"] * len(patient_ids))
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, patient_code, title, full_name, gender, date_of_birth, age_years
                         , panel_company
                         , tag
                         , contact_mobile
                         , alternate_mobile
                    FROM hpatient_master
                    WHERE id IN ({placeholders})
                    """,
                    patient_ids,
                )
                rows = {int(r["id"]): r for r in cur.fetchall()}

            response = []
            for item in selected:
                pid = int(item["patient_id"])
                if pid not in rows:
                    continue
                r = rows[pid]
                response.append(
                    {
                        "patient_id": pid,
                        "patient_code": r["patient_code"],
                        "full_name": f"{(r.get('title') or '').strip()} {(r['full_name'] or '').strip()}".strip(),
                        "tag": r.get("tag"),
                        "panel_company": r.get("panel_company"),
                        "gender": r["gender"],
                        "date_of_birth": r.get("date_of_birth").isoformat() if r.get("date_of_birth") else None,
                        "contact_mobile": r.get("contact_mobile") or None,
                        "alternate_mobile": r.get("alternate_mobile") or None,
                        "age": hage_label(r.get("age_years"), r.get("date_of_birth")),
                    }
                )
            return response
        finally:
            conn.close()

    def get_step1_bundle(self, caller_id: int, session):
        selected = session.get("hselected_patients", [])
        if not caller_id:
            return {
                "linked_patients": [],
                "selected_patients": [],
                "addresses": [],
                "selected_address_id": session.get("hselected_address_id"),
            }
        return {
            "linked_patients": self.get_linked_patients(caller_id, session),
            "selected_patients": self.get_selected_patients_enriched(caller_id, selected),
            "addresses": self.get_addresses_for_caller(caller_id),
            "selected_address_id": session.get("hselected_address_id"),
        }

    def list_colonies(self, city=None):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                if city:
                    cur.execute(
                        """
                        SELECT id, colony_name, pincode, route_no, city
                        FROM hcolony_master
                        WHERE is_active = 1 AND city = %s
                        ORDER BY colony_name
                        """,
                        (city,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, colony_name, pincode, route_no, city
                        FROM hcolony_master
                        WHERE is_active = 1
                        ORDER BY colony_name
                        """
                    )
                return cur.fetchall()
        finally:
            conn.close()

    def get_addresses_for_caller(self, caller_id: int):
        if not caller_id:
            return []

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT am.id, am.address_type, am.house_flat_no,
                           am.floor, am.street_line, am.landmark, am.colony_name_snapshot,
                           am.pincode_snapshot, am.route_no_snapshot, am.city, am.access_notes
                    FROM hcaller_patient_link cpl
                    INNER JOIN hpatient_address_link pal ON pal.patient_id = cpl.patient_id AND pal.is_active = 1
                    INNER JOIN haddress_master am ON am.id = pal.address_id
                    WHERE cpl.caller_id = %s AND cpl.is_active = 1
                    ORDER BY am.id DESC
                    """,
                    (caller_id,),
                )
                return cur.fetchall()
        finally:
            conn.close()

    def create_address_for_patients(self, patient_ids: list, payload: dict, actor_user_id=None):
        actor = self._actor(actor_user_id)
        house_flat_no = (payload.get("house_flat_no") or "").strip()
        colony_id = int(payload.get("colony_id", 0))
        if not house_flat_no or not colony_id:
            return {"ok": False, "message": "House/Flat and colony are required"}

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, colony_name, pincode, route_no, city FROM hcolony_master WHERE id=%s AND is_active=1",
                    (colony_id,),
                )
                colony = cur.fetchone()
                if not colony:
                    return {"ok": False, "message": "Invalid colony"}
                city = (payload.get("city") or "").strip()
                if not city:
                    return {"ok": False, "message": "City is required"}

                cur.execute(
                    """
                    INSERT INTO haddress_master
                    (address_type, house_flat_no, floor, street_line, landmark,
                     colony_id, colony_name_snapshot, pincode_snapshot, route_no_snapshot, city,
                     access_notes, created_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        payload.get("address_type") or "Home",
                        house_flat_no,
                        payload.get("floor") or None,
                        payload.get("street_line") or None,
                        payload.get("landmark") or None,
                        colony_id,
                        colony["colony_name"],
                        colony["pincode"],
                        colony["route_no"],
                        city,
                        payload.get("access_notes") or None,
                        actor,
                    ),
                )
                address_id = cur.lastrowid

                for index, patient_id in enumerate(patient_ids):
                    is_default = 1 if index == 0 else 0
                    if is_default == 1:
                        cur.execute(
                            "UPDATE hpatient_address_link SET is_default = 0 WHERE patient_id = %s",
                            (patient_id,),
                        )
                    cur.execute(
                        """
                        INSERT INTO hpatient_address_link
                        (patient_id, address_id, is_default, is_active, created_by)
                        VALUES (%s,%s,%s,1,%s)
                        ON DUPLICATE KEY UPDATE is_active=1
                        """,
                        (patient_id, address_id, is_default, actor),
                    )

                conn.commit()

                snapshot = {
                    "address_id": address_id,
                    "address_type": payload.get("address_type") or "Home",
                    "house_flat_no": house_flat_no,
                    "floor": payload.get("floor") or None,
                    "street_line": payload.get("street_line") or None,
                    "landmark": payload.get("landmark") or None,
                    "colony_name_snapshot": colony["colony_name"],
                    "pincode_snapshot": colony["pincode"],
                    "route_no_snapshot": colony["route_no"],
                    "city": city,
                    "access_notes": payload.get("access_notes") or None,
                }
                return {"ok": True, "address": {"id": address_id}, "address_snapshot": snapshot}
        except Exception as exc:
            conn.rollback()
            return {"ok": False, "message": str(exc)}
        finally:
            conn.close()

    def get_address_snapshot(self, address_id: int):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id AS address_id, address_type, house_flat_no, floor,
                           street_line, landmark, colony_name_snapshot, pincode_snapshot,
                           route_no_snapshot, city, access_notes
                    FROM haddress_master
                    WHERE id=%s
                    """,
                    (address_id,),
                )
                return cur.fetchone()
        finally:
            conn.close()

    def get_address_for_caller(self, caller_id: int, address_id: int):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT am.id, am.address_type, am.house_flat_no, am.floor,
                           am.street_line, am.landmark, am.colony_id, am.colony_name_snapshot,
                           am.pincode_snapshot, am.route_no_snapshot, am.city, am.access_notes
                    FROM hcaller_patient_link cpl
                    INNER JOIN hpatient_address_link pal ON pal.patient_id = cpl.patient_id AND pal.is_active = 1
                    INNER JOIN haddress_master am ON am.id = pal.address_id
                    WHERE cpl.caller_id = %s
                      AND cpl.is_active = 1
                      AND am.id = %s
                    LIMIT 1
                    """,
                    (caller_id, address_id),
                )
                return cur.fetchone()
        finally:
            conn.close()

    def update_address_for_caller(self, caller_id: int, address_id: int, payload: dict, actor_user_id=None):
        house_flat_no = (payload.get("house_flat_no") or "").strip()
        colony_id = int(payload.get("colony_id", 0))
        city = (payload.get("city") or "").strip()
        if not house_flat_no or not colony_id or not city:
            return {"ok": False, "message": "House/Flat, colony and city are required"}

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM hcaller_patient_link cpl
                    INNER JOIN hpatient_address_link pal ON pal.patient_id = cpl.patient_id AND pal.is_active = 1
                    WHERE cpl.caller_id = %s
                      AND cpl.is_active = 1
                      AND pal.address_id = %s
                    LIMIT 1
                    """,
                    (caller_id, address_id),
                )
                if not cur.fetchone():
                    return {"ok": False, "message": "Address not linked with selected caller"}

                cur.execute(
                    "SELECT id, colony_name, pincode, route_no FROM hcolony_master WHERE id=%s AND is_active=1",
                    (colony_id,),
                )
                colony = cur.fetchone()
                if not colony:
                    return {"ok": False, "message": "Invalid colony"}

                cur.execute(
                    """
                    UPDATE haddress_master
                    SET address_type=%s,
                        house_flat_no=%s,
                        floor=%s,
                        street_line=%s,
                        landmark=%s,
                        colony_id=%s,
                        colony_name_snapshot=%s,
                        pincode_snapshot=%s,
                        route_no_snapshot=%s,
                        city=%s,
                        access_notes=%s
                    WHERE id=%s
                    """,
                    (
                        payload.get("address_type") or "Home",
                        house_flat_no,
                        payload.get("floor") or None,
                        payload.get("street_line") or None,
                        payload.get("landmark") or None,
                        colony_id,
                        colony["colony_name"],
                        colony["pincode"],
                        colony["route_no"],
                        city,
                        payload.get("access_notes") or None,
                        address_id,
                    ),
                )

                conn.commit()
                snapshot = self.get_address_snapshot(address_id)
                return {"ok": True, "address": {"id": address_id}, "address_snapshot": snapshot}
        except Exception as exc:
            conn.rollback()
            return {"ok": False, "message": str(exc)}
        finally:
            conn.close()

    def _slot_start_key(self, slot_text: str):
        text = (slot_text or "").strip()
        if not text:
            return None
        lower = text.lower()
        if " to " in lower:
            token = re.split(r"\bto\b", text, flags=re.IGNORECASE)[0].strip()
        elif "-" in text:
            token = text.split("-", 1)[0].strip()
        else:
            token = text
        token = token.replace(".", "").upper().replace(" ", "")
        match = re.match(r"^(\d{1,2})(?::(\d{2}))?(AM|PM)$", token)
        if not match:
            return None
        hour = int(match.group(1))
        minute = int(match.group(2) or 0)
        ampm = match.group(3)
        if ampm == "PM" and hour != 12:
            hour += 12
        if ampm == "AM" and hour == 12:
            hour = 0
        return hour * 60 + minute

    def route_slot_grid_data(self, visit_date: str, selected_route: str | None = None):
        if not visit_date:
            return {"ok": False, "message": "Date is required"}

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT route_no AS route_name
                    FROM hcolony_master
                    WHERE is_active = 1 AND route_no IS NOT NULL AND route_no <> ''
                    ORDER BY route_no
                    """
                )
                routes = [row["route_name"] for row in cur.fetchall()]

                cur.execute(
                    """
                    SELECT hcb.id, hcb.preferred_time_slot, am.route_no_snapshot AS route_name,
                           am.city, am.colony_name_snapshot, cm.primary_mobile
                    FROM hhome_collection_booking hcb
                    INNER JOIN haddress_master am ON am.id = hcb.selected_address_id
                    INNER JOIN hcaller_master cm ON cm.id = hcb.caller_id
                    WHERE hcb.preferred_visit_date = %s
                      AND hcb.booking_status <> 4
                    ORDER BY hcb.id DESC
                    """,
                    (visit_date,),
                )
                rows = cur.fetchall()

            selected = (selected_route or "").strip()
            bookings = []
            route_set = set(routes)
            for row in rows:
                route_name = (row.get("route_name") or "UNASSIGNED").strip() or "UNASSIGNED"
                route_set.add(route_name)
                bookings.append(
                    {
                        "slot": row.get("preferred_time_slot"),
                        "slot_key": self._slot_start_key(row.get("preferred_time_slot") or ""),
                        "route": route_name,
                        "city": row.get("city") or "",
                        "area": row.get("colony_name_snapshot") or "",
                        "mobile": row.get("primary_mobile") or "",
                    }
                )

            ordered = sorted([r for r in route_set if r != "UNASSIGNED"], key=str.upper)
            if selected and selected in ordered:
                ordered = [selected] + [r for r in ordered if r != selected]
            elif selected and selected != "UNASSIGNED":
                ordered = [selected] + ordered
            if "UNASSIGNED" in route_set:
                ordered.append("UNASSIGNED")

            return {
                "ok": True,
                "date": visit_date,
                "selected_route": selected,
                "routes": ordered,
                "bookings": bookings,
                "total_bookings": len(bookings),
            }
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
        finally:
            conn.close()

    def search_panel_companies(self, query: str, limit: int = 15, atype: str | None = None):
        self.preload_panel_catalog()
        q = (query or "").strip()
        if len(q) < 2:
            return []
        limit = max(1, min(int(limit or 15), 50))
        ql = q.lower()
        candidates = self._panel_catalog["prefix2"].get(ql[:2], [])
        atype_code = self._norm_code(atype).upper()
        if atype_code in ("C", "D"):
            if atype_code == "C":
                candidates = [p for p in candidates if p.get("_has_c")]
            else:
                candidates = [p for p in candidates if p.get("_has_d")]
        prefix = [p for p in candidates if p["_pname_lc"].startswith(ql)]
        contains = [p for p in candidates if (ql in p["_pname_lc"]) and (not p["_pname_lc"].startswith(ql))]
        rows = (prefix + contains)[:limit]
        # Hide internal lowercase key before returning
        return [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]

    def panel_groups(self, comp_cat_id: str):
        self.preload_panel_catalog()
        ccid = (comp_cat_id or "").strip()
        if not ccid:
            return []
        groups = self._panel_catalog["groups_by_comp"].get(ccid, {})
        return [groups[k] for k in sorted(groups.keys())]

    def panel_subgroups(self, comp_cat_id: str, gcode: str):
        self.preload_panel_catalog()
        ccid = (comp_cat_id or "").strip()
        gc = (gcode or "").strip()
        if not ccid or not gc:
            return []
        subgroups = self._panel_catalog["subgroups_by_comp_g"].get((ccid, gc), {})
        return [subgroups[k] for k in sorted(subgroups.keys())]

    def panel_tests(self, comp_cat_id: str, gcode: str, scode: str):
        self.preload_panel_catalog()
        ccid = (comp_cat_id or "").strip()
        gc = (gcode or "").strip()
        sc = (scode or "").strip()
        if not ccid or not gc or not sc:
            return []
        return self._panel_catalog["tests_by_comp_g_s"].get((ccid, gc, sc), [])

    def panel_child_tests(self, parent_gcode: str, parent_scode: str, parent_test_code: str):
        self.preload_panel_catalog()
        pg = (parent_gcode or "").strip()
        ps = (parent_scode or "").strip()
        pt = (parent_test_code or "").strip()
        if not pg or not ps or not pt:
            return []

        rows = self._panel_catalog["profile_children_map"].get((pg, ps, pt), [])
        out = []
        for row in rows:
            child_key = (
                self._norm_code(row.get("gcode")),
                self._norm_code(row.get("scode")),
                self._norm_code(row.get("test_code")),
            )
            r = dict(row)
            r["has_children"] = bool(self._panel_catalog["profile_children_map"].get(child_key))
            out.append(r)
        return out

    def test_specimen_catalog(self):
        self.preload_panel_catalog()
        tests = {}
        for code1, row in self._panel_catalog["test_by_testcode1"].items():
            tc1 = self._norm_code(code1)
            if not tc1:
                continue
            tests[tc1] = {
                "description": self._norm_code(row.get("description")),
                "specimen_name": self._norm_code(row.get("specimen_name")),
            }

        children_by_testcode1 = {}
        for parent_key, rows in self._panel_catalog["profile_children_map"].items():
            parent = self._panel_catalog["test_by_g_s_testcode"].get(parent_key)
            parent_code1 = self._norm_code((parent or {}).get("testcode1"))
            if not parent_code1:
                continue
            children_by_testcode1.setdefault(parent_code1, [])
            for child in rows:
                child_code1 = self._norm_code(child.get("testcode1"))
                if child_code1 and child_code1 not in children_by_testcode1[parent_code1]:
                    children_by_testcode1[parent_code1].append(child_code1)

        return {
            "tests": tests,
            "children_by_testcode1": children_by_testcode1,
        }

    def confirm_booking(self, caller_id, selected_patients, selected_address_id, selected_snapshot, payload, actor_user_id=None):
        actor = self._actor(actor_user_id)
        if not caller_id:
            return {"ok": False, "message": "Caller is required"}
        if not selected_patients:
            return {"ok": False, "message": "Select at least one patient"}
        if not selected_address_id or not selected_snapshot:
            return {"ok": False, "message": "Select an address"}

        preferred_visit_date = payload.get("preferred_visit_date")
        preferred_time_slot = payload.get("preferred_time_slot")
        if not preferred_visit_date or not preferred_time_slot:
            return {"ok": False, "message": "Visit date and slot are required"}

        if preferred_visit_date < str(date.today()):
            return {"ok": False, "message": "Visit date cannot be in past"}

        tests_meta_map = payload.get("patient_tests_meta_map") or {}

        def _to_num(v):
            try:
                if v is None or v == "":
                    return None
                return float(v)
            except Exception:
                return None

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                tmp = self._temp_code("HHCB-TMP-")
                cur.execute(
                    """
                    INSERT INTO hhome_collection_booking
                    (booking_code, caller_id, selected_address_id, address_snapshot_json,
                     preferred_visit_date, preferred_time_slot, booking_status,
                     special_instructions, remarks, assigned_phlebotomist_id,
                     created_by, updated_by)
                    VALUES (%s,%s,%s,%s,%s,%s,0,%s,%s,NULL,%s,%s)
                    """,
                    (
                        tmp,
                        caller_id,
                        selected_address_id,
                        hto_json(selected_snapshot),
                        preferred_visit_date,
                        preferred_time_slot,
                        payload.get("special_instructions") or None,
                        payload.get("remarks") or None,
                        actor,
                        actor,
                    ),
                )
                booking_id = cur.lastrowid
                booking_code = hcode_from_id("HHCB-", booking_id)
                cur.execute("UPDATE hhome_collection_booking SET booking_code=%s WHERE id=%s", (booking_code, booking_id))

                seen_patients = set()
                for item in selected_patients:
                    pid = int(item["patient_id"])
                    if pid in seen_patients:
                        continue
                    seen_patients.add(pid)
                    cur.execute(
                        """
                        INSERT INTO hhome_collection_booking_patient
                        (booking_id, patient_id, created_by)
                        VALUES (%s,%s,%s)
                        """,
                        (booking_id, pid, actor),
                    )
                    booking_patient_id = cur.lastrowid

                    patient_meta = tests_meta_map.get(str(pid)) or tests_meta_map.get(pid) or {}
                    panel = patient_meta.get("panel") or {}
                    billing = patient_meta.get("billing") or {}
                    selected_tests = patient_meta.get("selected_tests") or []

                    for t in selected_tests:
                        booked_code = self._norm_code(t.get("booked_code") or t.get("test_code"))
                        if not booked_code:
                            continue
                        test_name = self._norm_code(t.get("description") or booked_code)
                        cur.execute(
                            """
                            INSERT INTO hhome_collection_booking_patient_test
                            (booking_id, booking_patient_id, patient_id, panel_company, comp_cat_id,
                             cat_details, gcode, scode, test_code, booked_code, test_name,
                             charge, mrp, max_discount, created_by)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON DUPLICATE KEY UPDATE
                            panel_company=VALUES(panel_company),
                            comp_cat_id=VALUES(comp_cat_id),
                            cat_details=VALUES(cat_details),
                            gcode=VALUES(gcode),
                            scode=VALUES(scode),
                            test_code=VALUES(test_code),
                            test_name=VALUES(test_name),
                            charge=VALUES(charge),
                            mrp=VALUES(mrp),
                            max_discount=VALUES(max_discount)
                            """,
                            (
                                booking_id,
                                booking_patient_id,
                                pid,
                                self._norm_code(panel.get("pname")),
                                self._norm_code(billing.get("comp_cat_id")),
                                self._norm_code(billing.get("cat_details")),
                                self._norm_code(t.get("gcode")),
                                self._norm_code(t.get("scode")),
                                self._norm_code(t.get("test_code")),
                                booked_code,
                                test_name,
                                _to_num(t.get("charge")),
                                _to_num(t.get("mrp")),
                                _to_num(t.get("max_discount")),
                                actor,
                            ),
                        )

                conn.commit()
                return {
                    "ok": True,
                    "booking_id": booking_id,
                    "booking_code": booking_code,
                    "print_url": f"/hhome-collection/print/{booking_id}",
                }
        except Exception as exc:
            conn.rollback()
            return {"ok": False, "message": str(exc)}
        finally:
            conn.close()

    def dashboard_rows(self, params):
        filters = []
        values = []

        if params.get("date_from"):
            filters.append("hcb.preferred_visit_date >= %s")
            values.append(params["date_from"])
        if params.get("date_to"):
            filters.append("hcb.preferred_visit_date <= %s")
            values.append(params["date_to"])
        if params.get("status") is not None and str(params.get("status")).strip() != "":
            status_raw = str(params.get("status")).strip()
            status_map = {
                "Pending": 0,
                "Assigned": 1,
                "Started": 2,
                "Completed": 3,
                "Cancelled": 4,
            }
            status_val = status_map.get(status_raw, status_raw)
            filters.append("hcb.booking_status = %s")
            values.append(status_val)
        if params.get("route"):
            filters.append("am.route_no_snapshot = %s")
            values.append(params["route"])
        if params.get("search"):
            filters.append("(hcb.booking_code LIKE %s OR cm.primary_mobile LIKE %s)")
            values.extend([f"%{params['search']}%", f"%{params['search']}%"])

        where_clause = "WHERE " + " AND ".join(filters) if filters else ""

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT hcb.id, hcb.preferred_visit_date, hcb.preferred_time_slot,
                           hcb.booking_status, cm.full_name AS caller_name, cm.primary_mobile,
                           am.colony_name_snapshot, am.route_no_snapshot,
                           COUNT(hcbp.id) AS patient_count,
                           GROUP_CONCAT(
                             DISTINCT TRIM(CONCAT_WS(' ', p.title, p.full_name))
                             ORDER BY p.full_name SEPARATOR ', '
                           ) AS patient_names
                    FROM hhome_collection_booking hcb
                    INNER JOIN hcaller_master cm ON cm.id = hcb.caller_id
                    INNER JOIN haddress_master am ON am.id = hcb.selected_address_id
                    LEFT JOIN hhome_collection_booking_patient hcbp ON hcbp.booking_id = hcb.id
                    LEFT JOIN hpatient_master p ON p.id = hcbp.patient_id
                    {where_clause}
                    GROUP BY hcb.id
                    ORDER BY hcb.id DESC
                    """,
                    values,
                )
                return cur.fetchall()
        finally:
            conn.close()

    def get_booking_full(self, booking_id: int):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT hcb.*, cm.full_name AS caller_name, cm.primary_mobile, cm.caller_code,
                           am.house_flat_no, am.floor, am.street_line, am.landmark,
                           am.colony_name_snapshot, am.pincode_snapshot, am.route_no_snapshot,
                           am.city
                    FROM hhome_collection_booking hcb
                    INNER JOIN hcaller_master cm ON cm.id = hcb.caller_id
                    INNER JOIN haddress_master am ON am.id = hcb.selected_address_id
                    WHERE hcb.id=%s
                    """,
                    (booking_id,),
                )
                booking = cur.fetchone()
                if not booking:
                    return None

                cur.execute(
                    """
                    SELECT p.id AS patient_id, p.patient_code, CONCAT_WS(' ', p.title, p.full_name) AS full_name
                    FROM hhome_collection_booking_patient hcbp
                    INNER JOIN hpatient_master p ON p.id = hcbp.patient_id
                    WHERE hcbp.booking_id=%s
                    ORDER BY p.full_name
                    """,
                    (booking_id,),
                )
                patients = cur.fetchall()

                cur.execute(
                    """
                    SELECT patient_id, booked_code, test_name
                    FROM hhome_collection_booking_patient_test
                    WHERE booking_id=%s
                    ORDER BY id
                    """,
                    (booking_id,),
                )
                test_rows = cur.fetchall()
                tests_by_patient = {}
                for row in test_rows:
                    pid = int(row.get("patient_id") or 0)
                    if pid <= 0:
                        continue
                    label = self._norm_code(row.get("test_name")) or self._norm_code(row.get("booked_code"))
                    if not label:
                        continue
                    tests_by_patient.setdefault(pid, [])
                    if label not in tests_by_patient[pid]:
                        tests_by_patient[pid].append(label)

                for p in patients:
                    pid = int(p.get("patient_id") or 0)
                    p["tests_display"] = ", ".join(tests_by_patient.get(pid, [])) if pid else ""

                booking["patients"] = patients
                return booking
        finally:
            conn.close()

    def search_active_staff_users(self, q: str, limit: int = 20):
        query = (q or "").strip()
        if len(query) < 2:
            return []

        try:
            safe_limit = max(1, min(int(limit or 20), 20))
        except Exception:
            safe_limit = 20

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, TRIM(name) AS name
                    FROM users
                    WHERE status='Active'
                      AND name IS NOT NULL
                      AND TRIM(name) <> ''
                      AND LOWER(TRIM(name)) LIKE LOWER(%s)
                    ORDER BY name
                    LIMIT {safe_limit}
                    """,
                    (f"%{query}%",),
                )
                return cur.fetchall() or []
        finally:
            conn.close()
    def get_phlebotomists(self):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name AS full_name, contact
                    FROM users
                    WHERE status='Active'
                      AND (
                        designation='Home Collection Phlebo'
                        OR role='phlebotomist'
                      )
                    ORDER BY name
                    """
                )
                return cur.fetchall()
        except Exception:
            return []
        finally:
            conn.close()

    def assign_phlebotomist(self, booking_id: int, user_id: int, actor_user_id=None):
        if booking_id <= 0 or user_id <= 0:
            return {"ok": False, "message": "booking_id and user_id are required"}

        actor = self._actor(actor_user_id)
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, assigned_phlebotomist_id, booking_status
                    FROM hhome_collection_booking
                    WHERE id=%s
                    LIMIT 1
                    """,
                    (booking_id,),
                )
                row = cur.fetchone()
                if not row:
                    return {"ok": False, "message": "Booking not found"}

                if row.get("assigned_phlebotomist_id"):
                    return {"ok": False, "message": "Booking already assigned"}

                if int(row.get("booking_status") or 0) not in (0, 1, 2):
                    return {"ok": False, "message": "Booking is not assignable"}

                cur.execute(
                    """
                    UPDATE hhome_collection_booking
                    SET assigned_phlebotomist_id=%s, booking_status=1, updated_by=%s
                    WHERE id=%s
                    """,
                    (user_id, actor, booking_id),
                )
                # Keep patient-level status in sync on booking assign.
                cur.execute(
                    """
                    UPDATE hhome_collection_booking_patient
                    SET booking_patient_status=1
                    WHERE booking_id=%s AND booking_patient_status=0
                    """,
                    (booking_id,),
                )
                conn.commit()
                return {"ok": True}
        except Exception as exc:
            conn.rollback()
            return {"ok": False, "message": str(exc)}
        finally:
            conn.close()

    def cancel_booking(self, booking_id: int, actor_user_id=None):
        if booking_id <= 0:
            return {"ok": False, "message": "booking_id is required"}

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE hhome_collection_booking SET booking_status=4, updated_by=%s WHERE id=%s",
                    (self._actor(actor_user_id), booking_id),
                )
                conn.commit()
                return {"ok": True}
        except Exception as exc:
            conn.rollback()
            return {"ok": False, "message": str(exc)}
        finally:
            conn.close()

    def assignment_planner_data(self, plan_date: str | None):
        target_date = (plan_date or "").strip()
        if not target_date:
            target_date = str(date.today())

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Active route list from colony master (base columns for planner)
                cur.execute(
                    """
                    SELECT DISTINCT route_no
                    FROM hcolony_master
                    WHERE is_active = 1
                      AND route_no IS NOT NULL
                      AND TRIM(route_no) <> ''
                    ORDER BY route_no
                    """
                )
                base_routes = [self._norm_code(r.get("route_no")) for r in cur.fetchall() if self._norm_code(r.get("route_no"))]

                # One-shot booking grid payload (avoids N+1)
                cur.execute(
                    """
                    SELECT
                        hcb.id AS booking_id,
                        hcb.preferred_time_slot,
                        hcb.booking_status,
                        COALESCE(NULLIF(TRIM(am.route_no_snapshot), ''), 'UNASSIGNED') AS route_name,
                        hcb.assigned_phlebotomist_id,
                        am.colony_name_snapshot,
                        am.city,
                        cm.primary_mobile AS caller_mobile,
                        COUNT(DISTINCT hbp.patient_id) AS patient_count
                    FROM hhome_collection_booking hcb
                    INNER JOIN haddress_master am ON am.id = hcb.selected_address_id
                    INNER JOIN hcaller_master cm ON cm.id = hcb.caller_id
                    LEFT JOIN hhome_collection_booking_patient hbp ON hbp.booking_id = hcb.id
                    WHERE hcb.preferred_visit_date = %s
                      AND hcb.booking_status IN (0, 1, 2)
                    GROUP BY
                        hcb.id,
                        hcb.preferred_time_slot,
                        hcb.booking_status,
                        route_name,
                        hcb.assigned_phlebotomist_id,
                        am.colony_name_snapshot,
                        am.city,
                        cm.primary_mobile
                    ORDER BY hcb.id DESC
                    """,
                    (target_date,),
                )
                rows = cur.fetchall()

            route_set = set(base_routes)
            grid_rows = []
            for r in rows:
                route_name = self._norm_code(r.get("route_name")) or "UNASSIGNED"
                route_set.add(route_name)
                slot_text = self._norm_code(r.get("preferred_time_slot"))
                grid_rows.append(
                    {
                        "booking_id": r.get("booking_id"),
                        "slot": slot_text,
                        "slot_key": self._slot_start_key(slot_text) or 9999,
                        "route_name": route_name,
                        "booking_status": int(r.get("booking_status") or 0),
                        "assigned_user_id": r.get("assigned_phlebotomist_id"),
                        "colony_name_snapshot": self._norm_code(r.get("colony_name_snapshot")),
                        "city": self._norm_code(r.get("city")),
                        "caller_mobile": self._norm_code(r.get("caller_mobile")),
                        "patient_count": int(r.get("patient_count") or 0),
                    }
                )

            ordered_routes = sorted([x for x in route_set if x and x != "UNASSIGNED"], key=str.upper)
            if "UNASSIGNED" in route_set:
                ordered_routes.append("UNASSIGNED")

            phlebos = self.get_phlebotomists()
            return {
                "ok": True,
                "date": target_date,
                "routes": ordered_routes,
                "rows": grid_rows,
                "phlebos": phlebos,
            }
        finally:
            conn.close()

    def commit_assignment_plan(self, plan_date: str, assignments: list, actor_user_id=None):
        target_date = (plan_date or "").strip()
        if not target_date:
            return {"ok": False, "message": "plan_date is required"}
        if not isinstance(assignments, list) or not assignments:
            return {"ok": False, "message": "assignments are required"}

        normalized = []
        for item in assignments:
            try:
                booking_id = int(item.get("booking_id", 0))
                user_id = int(item.get("assigned_user_id", 0))
            except Exception:
                continue
            if booking_id <= 0 or user_id <= 0:
                continue
            normalized.append(
                {
                    "booking_id": booking_id,
                    "assigned_user_id": user_id,
                    "grouped_route": self._norm_code(item.get("grouped_route")),
                }
            )
        if not normalized:
            return {"ok": False, "message": "No valid booking assignments provided"}

        actor = self._actor(actor_user_id)
        booking_ids = sorted({x["booking_id"] for x in normalized})
        booking_to_user = {x["booking_id"]: x["assigned_user_id"] for x in normalized}

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(booking_ids))
                cur.execute(
                    f"""
                    SELECT id
                    FROM hhome_collection_booking
                    WHERE id IN ({placeholders})
                      AND preferred_visit_date = %s
                      AND booking_status IN (0, 1, 2)
                      AND (assigned_phlebotomist_id IS NULL OR assigned_phlebotomist_id = 0)
                    """,
                    booking_ids + [target_date],
                )
                valid_ids = {int(r["id"]) for r in cur.fetchall()}
                if not valid_ids:
                    return {"ok": False, "message": "All selected bookings are already assigned or not assignable"}

                update_rows = [(booking_to_user[bid], actor, bid) for bid in booking_ids if bid in valid_ids]
                cur.executemany(
                    """
                    UPDATE hhome_collection_booking
                    SET assigned_phlebotomist_id=%s, booking_status=1, updated_by=%s
                    WHERE id=%s
                    """,
                    update_rows,
                )

                cur.execute(
                    f"""
                    UPDATE hhome_collection_booking_patient
                    SET booking_patient_status = 1
                    WHERE booking_id IN ({",".join(["%s"] * len(valid_ids))})
                      AND booking_patient_status = 0
                    """,
                    list(valid_ids),
                )

                # Message preview payload (no DB route/address changes here)
                cur.execute(
                    f"""
                    SELECT
                        b.id AS booking_id,
                        u.name AS phlebo_name,
                        p.full_name AS patient_name,
                        p.contact_mobile
                    FROM hhome_collection_booking b
                    INNER JOIN users u ON u.id = b.assigned_phlebotomist_id
                    INNER JOIN hhome_collection_booking_patient bp ON bp.booking_id = b.id
                    INNER JOIN hpatient_master p ON p.id = bp.patient_id
                    WHERE b.id IN ({",".join(["%s"] * len(valid_ids))})
                    ORDER BY b.id
                    """,
                    list(valid_ids),
                )
                msg_rows = cur.fetchall()

                conn.commit()

            preview = []
            by_booking = {}
            for r in msg_rows:
                bid = int(r.get("booking_id") or 0)
                if bid <= 0:
                    continue
                by_booking.setdefault(
                    bid,
                    {
                        "booking_id": bid,
                        "phlebo_name": self._norm_code(r.get("phlebo_name")),
                        "recipients": set(),
                        "patient_names": [],
                    },
                )
                pname = self._norm_code(r.get("patient_name"))
                pmob = self._norm_code(r.get("contact_mobile"))
                if pname and pname not in by_booking[bid]["patient_names"]:
                    by_booking[bid]["patient_names"].append(pname)
                if pmob:
                    by_booking[bid]["recipients"].add(pmob)

            for _, item in sorted(by_booking.items(), key=lambda x: x[0]):
                patient_line = ", ".join(item["patient_names"]) if item["patient_names"] else "Patient"
                lines = [
                    f"Hey {patient_line}",
                    "Your pickup is successfully assigned.",
                    f"Phlebo {item['phlebo_name']} will be arrived soon.",
                ]
                preview.append(
                    {
                        "booking_id": item["booking_id"],
                        "phlebo_name": item["phlebo_name"],
                        "targets": sorted(item["recipients"]),
                        "message_text": "\n".join(lines),
                    }
                )

            # Best-effort WhatsApp send using existing project helper.
            sent = 0
            failed = 0
            send_results = []
            try:
                from app.alerts import send_whatsapp_to_number
            except Exception:
                send_whatsapp_to_number = None

            if send_whatsapp_to_number:
                for msg in preview:
                    text = self._norm_code(msg.get("message_text"))
                    targets = msg.get("targets") or []
                    if not text or not targets:
                        failed += 1
                        send_results.append(
                            {
                                "booking_id": msg.get("booking_id"),
                                "target": "",
                                "status_code": 0,
                                "response": "Missing target(s) or message",
                            }
                        )
                        continue
                    for raw_target in targets:
                        target = self._normalize_wa_target(raw_target)
                        if not target:
                            failed += 1
                            send_results.append(
                                {
                                    "booking_id": msg.get("booking_id"),
                                    "target": raw_target,
                                    "status_code": 0,
                                    "response": "Invalid target mobile",
                                }
                            )
                            continue
                        try:
                            status_code, response_text = send_whatsapp_to_number(target, text)
                            ok = int(status_code) in (200, 201)
                            sent += 1 if ok else 0
                            failed += 0 if ok else 1
                            send_results.append(
                                {
                                    "booking_id": msg.get("booking_id"),
                                    "target": target,
                                    "status_code": status_code,
                                    "response": response_text,
                                }
                            )
                        except Exception as exc:
                            failed += 1
                            send_results.append(
                                {
                                    "booking_id": msg.get("booking_id"),
                                    "target": target,
                                    "status_code": 500,
                                    "response": str(exc),
                                }
                            )

            return {
                "ok": True,
                "updated_count": len(valid_ids),
                "skipped_assigned_count": max(len(booking_ids) - len(valid_ids), 0),
                "messages_preview": preview,
                "send_summary": {"sent": sent, "failed": failed},
                "send_results": send_results,
            }
        except Exception as exc:
            conn.rollback()
            return {"ok": False, "message": str(exc)}
        finally:
            conn.close()


