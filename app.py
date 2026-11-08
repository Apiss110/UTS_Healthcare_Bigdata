#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Healthcare Insurance Claims Analytics - Flask dashboard
IFB-451 BIG DATA

Struktur proyek:
    app.py                      <- file ini (dashboard)
    main.py                     <- pipeline PySpark (menulis data/processed dan output/)
    templates/index.html        <- frontend
    data/raw/Dataset_HealthcareInsurance.csv     <- input klaim
    data/raw/Dataset_HealthcareInsurance.json    <- input profil pasien (JSON Lines)
    data/processed/             <- dataset BERSIH hasil pipeline
    output/                     <- flagged_claims.csv, summary_cpt_risk.csv, provider_totals.csv, pipeline_metrics.json

Jalankan:   python app.py      lalu buka http://127.0.0.1:5000
"""
import json
import math
import os
import subprocess
import sys
import threading
import time
from datetime import datetime

import pandas as pd
from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

# ----------------------------------------------------------------------------
# KONFIGURASI
# ----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
MAIN_PY = os.path.join(BASE_DIR, "main.py")

RAW_CSV = os.path.join(RAW_DIR, "Dataset_HealthcareInsurance.csv")
RAW_JSON = os.path.join(RAW_DIR, "Dataset_HealthcareInsurance.json")
CLEAN_CSV = os.path.join(PROCESSED_DIR, "claims_clean.csv")
QUARANTINE_CSV = os.path.join(PROCESSED_DIR, "claims_quarantine.csv")
CLEAN_PATIENTS_JSON = os.path.join(PROCESSED_DIR, "patients_clean.json")
FLAGGED_CSV = os.path.join(OUTPUT_DIR, "flagged_claims.csv")
SUMMARY_CSV = os.path.join(OUTPUT_DIR, "summary_cpt_risk.csv")
PROVIDER_CSV = os.path.join(OUTPUT_DIR, "provider_totals.csv")
METRICS_JSON = os.path.join(OUTPUT_DIR, "pipeline_metrics.json")
RUN_INFO_JSON = os.path.join(OUTPUT_DIR, "dashboard_last_run.json")
RUN_LOG = os.path.join(OUTPUT_DIR, "pipeline_last_run.log")

PIPELINE_TIMEOUT_S = int(os.environ.get("PIPELINE_TIMEOUT_S", "1800"))
LOG_TAIL_LINES = 80

# Ambang aturan flag - HARUS sama dengan konstanta di main.py
Z_THRESHOLD = 2.0
RATIO_THRESHOLD = 5.0
RARE_SPECIALTY_SHARE = 0.15
BURST_30D = 10

CLAIM_COLS = ["CLAIM_ID", "PATIENT_ID", "DATE_OF_SERVICE", "CPT_CODE", "CPT_DESCRIPTION",
              "PROVIDER_NPI", "PROVIDER_SPECIALTY", "TOTAL_PAID_BY_INSURANCE", "PATIENT_COPAYMENT"]
CRITICAL = ["CLAIM_ID", "PATIENT_ID", "DATE_OF_SERVICE", "CPT_CODE", "PROVIDER_NPI",
            "TOTAL_PAID_BY_INSURANCE", "PATIENT_COPAYMENT", "PROVIDER_SPECIALTY"]
PATIENT_FIELDS = ["PATIENT_ID", "PATIENT_STATE", "PRODUCT_LINE"]

# Nilai acuan (profil dataset di data/raw) - dipakai hanya sebelum pipeline pernah dijalankan
STATIC_REFERENCE = {"rows_raw": 942, "rows_real": 906, "scale_factor": 1000, "patients_real": 10,
                    "issue_pct_before": 6.16, "issue_pct_after": 0.11}

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "ifb451-claims-dashboard-local")
RUN_LOCK = threading.Lock()


# ----------------------------------------------------------------------------
# UTILITAS
# ----------------------------------------------------------------------------
def _py(v):
    """Nilai pandas/numpy -> tipe Python aman JSON (NaN -> None)."""
    if v is None:
        return None
    if hasattr(v, "item"):
        try:
            v = v.item()
        except Exception:
            pass
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _records(df):
    return [{k: _py(v) for k, v in rec.items()} for rec in df.to_dict("records")]


def _read_csv(path):
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path)
    except Exception as exc:
        app.logger.warning("Gagal membaca %s: %s", path, exc)
        return None


def _read_jsonl(path):
    if not os.path.exists(path):
        return None
    try:
        return pd.read_json(path, lines=True, dtype=False)
    except Exception as exc:
        app.logger.warning("Gagal membaca %s: %s", path, exc)
        return None


def _read_json(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        app.logger.warning("Gagal membaca %s: %s", path, exc)
        return None


def _tail(path, n=LOG_TAIL_LINES):
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return "".join(fh.readlines()[-n:])
    except Exception:
        return ""


def _scan_processed():
    out = []
    if not os.path.isdir(PROCESSED_DIR):
        return out
    for name in sorted(os.listdir(PROCESSED_DIR)):
        path = os.path.join(PROCESSED_DIR, name)
        if os.path.isfile(path):
            out.append({"file": f"data/processed/{name}", "rows": None, "kb": round(os.path.getsize(path) / 1024, 1), "desc": ""})
        elif os.path.isdir(path):
            out.append({"file": f"data/processed/{name}/", "rows": None, "kb": None, "desc": ""})
    return out


def _explain_flags(row):
    """Alasan flag per klaim, diturunkan dari kolom flagged_claims.csv (aturan sama dengan main.py)."""
    reasons = []
    ratio, z = row.get("ratio_to_median"), row.get("z_score")
    share, burst = row.get("specialty_share"), row.get("claims_last_30d")
    if ratio is not None and ratio >= RATIO_THRESHOLD:
        reasons.append(f"{ratio:.1f}x median CPT")
    if z is not None and z >= Z_THRESHOLD:
        reasons.append(f"z-score {z:.1f}")
    if share is not None and share < RARE_SPECIALTY_SHARE:
        reasons.append(f"spesialisasi jarang ({share * 100:.0f}% klaim CPT)")
    if burst is not None and burst >= BURST_30D:
        reasons.append(f"{int(burst)} klaim dalam 30 hari")
    n_flags = int(row.get("n_flags") or 0)
    if n_flags > len(reasons):
        reasons.append("biaya pasien > bayar asuransi")
    return reasons


def _derive_rules(flagged):
    """Jumlah klaim per aturan flag, dihitung dari flagged_claims.csv (aturan ke-5 = sisa n_flags)."""
    f = flagged
    r_ratio = (f["ratio_to_median"] >= RATIO_THRESHOLD).fillna(False)
    r_z = (f["z_score"] >= Z_THRESHOLD).fillna(False)
    r_rare = (f["specialty_share"] < RARE_SPECIALTY_SHARE).fillna(False)
    r_burst = (f["claims_last_30d"] >= BURST_30D).fillna(False)
    derived = r_ratio.astype(int) + r_z.astype(int) + r_rare.astype(int) + r_burst.astype(int)
    r_copay = (f["n_flags"] - derived) > 0
    return {"flag_price_ratio": int(r_ratio.sum()), "flag_zscore": int(r_z.sum()), "flag_rare_specialty": int(r_rare.sum()),
            "flag_burst_30d": int(r_burst.sum()), "flag_copay_gt_paid": int(r_copay.sum())}


# ----------------------------------------------------------------------------
# DATA DASHBOARD
# ----------------------------------------------------------------------------
def build_dashboard_data():
    flagged = _read_csv(FLAGGED_CSV)
    summary = _read_csv(SUMMARY_CSV)
    provider = _read_csv(PROVIDER_CSV)
    metrics = _read_json(METRICS_JSON)
    run_info = _read_json(RUN_INFO_JSON)

    has_output = flagged is not None and summary is not None
    data = {"has_output": has_output, "has_metrics": metrics is not None,
            "generated_at": (metrics or {}).get("generated_at"), "last_run": run_info}

    # ---- Volume ----
    vol_m = (metrics or {}).get("volume") or {}
    scale = int((metrics or {}).get("scale_factor") or STATIC_REFERENCE["scale_factor"])
    rows_real = vol_m.get("rows_real")
    if rows_real is None and summary is not None:
        rows_real = int(summary["n_claims"].sum())
    rows_real = int(rows_real or STATIC_REFERENCE["rows_real"])
    patients_real = int(vol_m.get("patients_real") or STATIC_REFERENCE["patients_real"])
    data["volume"] = {
        "rows_raw": int(vol_m.get("rows_raw") or STATIC_REFERENCE["rows_raw"]),
        "rows_real": rows_real,
        "rows_scaled": int(vol_m.get("rows_scaled") or rows_real * scale),
        "patients_real": patients_real,
        "patients_scaled": int(vol_m.get("patients_scaled") or patients_real * scale),
        "scale_factor": scale,
        "partitions_scaled": vol_m.get("partitions_scaled"),
        "source": "pipeline" if vol_m else "static",
    }

    # ---- Velocity ----
    bench = (metrics or {}).get("benchmark")
    batches = ((metrics or {}).get("velocity") or {}).get("batches") or []
    data["velocity"] = {"benchmark": bench, "batches": batches} if (bench or batches) else None

    # ---- Veracity ----
    ver = (metrics or {}).get("veracity")
    if ver:
        data["veracity"] = {
            "before_pct": ver["before"]["issue_rows_pct"], "after_pct": ver["after"]["issue_rows_pct"],
            "before": ver["before"], "after": ver["after"],
            "quarantined_rows": ver.get("quarantined_rows"), "quarantine_reasons": ver.get("quarantine_reasons"),
            "duplicates_removed": ver.get("duplicates_removed"), "specialty_imputed": ver.get("specialty_imputed"),
            "conflicting_duplicates": ver.get("conflicting_duplicates"),
            "patients_total": ver.get("patients_total"), "patients_state_null": ver.get("patients_state_null"),
            "cleaning_latency_s": ver.get("cleaning_latency_s"), "source": "pipeline",
        }
    else:
        data["veracity"] = {
            "before_pct": STATIC_REFERENCE["issue_pct_before"], "after_pct": STATIC_REFERENCE["issue_pct_after"],
            "before": None, "after": None, "quarantined_rows": None, "quarantine_reasons": None,
            "duplicates_removed": None, "specialty_imputed": None, "conflicting_duplicates": None,
            "patients_total": None, "patients_state_null": None, "cleaning_latency_s": None, "source": "static",
        }
    data["processed_files"] = (metrics or {}).get("processed_files") or _scan_processed()

    # ---- Value ----
    if has_output:
        total_claims = int(summary["n_claims"].sum())
        total_paid = int(summary["total_paid"].sum())
        n_flagged = int(len(flagged))
        paid_flagged = int(flagged["TOTAL_PAID_BY_INSURANCE"].sum())

        by_cpt = (summary.groupby(["CPT_CODE", "CPT_DESCRIPTION"], as_index=False)
                  .agg(n_claims=("n_claims", "sum"), n_flagged=("n_flagged", "sum"), flagged_paid=("flagged_paid", "sum"))
                  .query("n_flagged > 0"))
        top = flagged.sort_values(["n_flags", "TOTAL_PAID_BY_INSURANCE"], ascending=[False, False]).head(10)
        top_claims = []
        for rank, rec in enumerate(_records(top), start=1):
            rec["rank"] = rank
            rec["reasons"] = _explain_flags(rec)
            top_claims.append(rec)

        providers = None
        if provider is not None and len(provider):
            p = provider.sort_values("total_paid", ascending=False).copy()
            p["label"] = p["PROVIDER_SPECIALTY"].astype(str) + " (..." + p["PROVIDER_NPI"].astype(str).str[-4:] + ")"
            providers = _records(p)

        data["value"] = {
            "total_claims": total_claims, "flagged_claims": n_flagged,
            "flagged_pct": round(100.0 * n_flagged / total_claims, 1) if total_claims else 0,
            "paid_total": total_paid, "paid_flagged": paid_flagged,
            "paid_flagged_pct": round(100.0 * paid_flagged / total_paid, 1) if total_paid else 0,
            "avg_paid": round(total_paid / total_claims, 2) if total_claims else 0,
            "top_by_value": _records(by_cpt.sort_values(["flagged_paid", "n_flagged"], ascending=False).head(5)),
            "top_by_count": _records(by_cpt.sort_values(["n_flagged", "flagged_paid"], ascending=False).head(5)),
            "top_claims": top_claims, "rules": _derive_rules(flagged), "providers": providers,
            "joint_replacement": (metrics or {}).get("joint_replacement"),
        }
    else:
        data["value"] = None
    return data


# ----------------------------------------------------------------------------
# PENGUJIAN VERIFIKASI (VER-01 s/d VER-07) - dibaca langsung dari berkas
# ----------------------------------------------------------------------------
def run_verification():
    tests = []

    def add(tid, name, status, metric, detail):
        tests.append({"id": tid, "name": name, "status": status, "metric": metric, "detail": detail})

    raw = _read_csv(RAW_CSV)
    pat = _read_jsonl(RAW_JSON)
    clean = _read_csv(CLEAN_CSV)
    quar = _read_csv(QUARANTINE_CSV)
    flagged, summary = _read_csv(FLAGGED_CSV), _read_csv(SUMMARY_CSV)

    # VER-01
    ok = raw is not None and pat is not None
    add("VER-01", "Ketersediaan data input (data/raw)", "PASS" if ok else "FAIL",
        f"CSV {len(raw)} baris, JSON {len(pat)} objek" if ok else "berkas tidak lengkap",
        "CSV klaim dan JSON profil pasien terbaca." if ok else "Letakkan Dataset_HealthcareInsurance.csv dan .json di data/raw/.")
    # VER-02
    if raw is not None:
        miss = [c for c in CLAIM_COLS if c not in raw.columns]
        add("VER-02", "Skema CSV klaim", "PASS" if not miss else "FAIL", f"{len(raw.columns)} kolom",
            "Sembilan kolom wajib tersedia." if not miss else "Kolom hilang: " + ", ".join(miss))
    else:
        add("VER-02", "Skema CSV klaim", "FAIL", "-", "CSV tidak terbaca.")
    # VER-03
    if pat is not None:
        miss = [c for c in PATIENT_FIELDS if c not in pat.columns]
        add("VER-03", "Skema JSON profil pasien", "PASS" if not miss else "FAIL", f"{len(pat.columns)} field, {len(pat)} objek",
            "Field PATIENT_ID, PATIENT_STATE, PRODUCT_LINE tersedia." if not miss else "Field hilang: " + ", ".join(miss))
    else:
        add("VER-03", "Skema JSON profil pasien", "FAIL", "-", "JSON tidak terbaca.")
    # VER-04
    if raw is not None and pat is not None and "PATIENT_ID" in raw and "PATIENT_ID" in pat:
        orphan = sorted(set(raw["PATIENT_ID"]) - set(pat["PATIENT_ID"]))
        add("VER-04", "Integritas kunci integrasi (PATIENT_ID)", "PASS" if not orphan else "FAIL",
            f"{len(orphan)} orphan key", "Setiap PATIENT_ID di klaim ada di profil pasien, join aman." if not orphan else "Tidak ada di JSON: " + ", ".join(orphan[:5]))
    else:
        add("VER-04", "Integritas kunci integrasi (PATIENT_ID)", "FAIL", "-", "Data input tidak lengkap.")
    # VER-05
    if clean is not None:
        dup = int(clean["CLAIM_ID"].duplicated().sum())
        nul = int(clean[[c for c in CRITICAL if c in clean.columns]].isna().any(axis=1).sum())
        neg = int((clean["TOTAL_PAID_BY_INSURANCE"] <= 0).sum())
        good = dup == 0 and nul == 0 and neg == 0
        add("VER-05", "Kualitas data bersih (data/processed)", "PASS" if good else "FAIL",
            f"duplikat {dup}, kosong {nul}, bayar <= 0: {neg}", "Data bersih bebas duplikat, nilai kosong, dan tagihan nol/negatif." if good else "Masih ada masalah kualitas pada claims_clean.csv.")
    else:
        add("VER-05", "Kualitas data bersih (data/processed)", "PENDING", "-", "Jalankan pipeline untuk menghasilkan data bersih.")
    # VER-06
    if raw is not None and clean is not None and quar is not None:
        ok = len(raw) == len(clean) + len(quar)
        add("VER-06", "Akuntansi baris (tidak ada data hilang)", "PASS" if ok else "FAIL",
            f"{len(raw)} = {len(clean)} bersih + {len(quar)} karantina", "Setiap baris mentah berakhir sebagai data bersih atau tercatat di karantina." if ok else "Jumlah baris tidak cocok.")
    else:
        add("VER-06", "Akuntansi baris (tidak ada data hilang)", "PENDING", "-", "Jalankan pipeline untuk menghasilkan data bersih dan karantina.")
    # VER-07
    if clean is not None and flagged is not None and summary is not None:
        n_sum = int(summary["n_claims"].sum())
        ok = n_sum == len(clean) and len(flagged) <= len(clean)
        add("VER-07", "Konsistensi hasil analitik Spark dengan data bersih", "PASS" if ok else "FAIL",
            f"dievaluasi {n_sum}, bersih {len(clean)}, ter-flag {len(flagged)}", "Jumlah klaim yang dianalisis sama dengan data bersih." if ok else "Jumlah klaim analitik berbeda dari data bersih.")
    else:
        add("VER-07", "Konsistensi hasil analitik Spark dengan data bersih", "PENDING", "-", "Jalankan pipeline untuk menghasilkan output analitik.")
    return tests


PREVIEW_SOURCES = {
    "raw_claims": (RAW_CSV, "csv"), "raw_patients": (RAW_JSON, "jsonl"),
    "clean_claims": (CLEAN_CSV, "csv"), "quarantine": (QUARANTINE_CSV, "csv"),
    "clean_patients": (CLEAN_PATIENTS_JSON, "jsonl"),
}


# ----------------------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html", data=build_dashboard_data(), log_tail=_tail(RUN_LOG))


@app.route("/api/run-tests")
def api_run_tests():
    try:
        return jsonify({"success": True, "tests": run_verification()})
    except Exception as exc:
        app.logger.exception("run_verification gagal")
        return jsonify({"success": False, "error": str(exc)}), 500


@app.route("/api/data-preview")
def api_data_preview():
    kind = request.args.get("type", "raw_claims")
    try:
        limit = max(1, min(int(request.args.get("limit", 12)), 100))
    except ValueError:
        limit = 12
    if kind not in PREVIEW_SOURCES:
        return jsonify({"success": False, "error": "Jenis data tidak dikenal."}), 400
    path, fmt = PREVIEW_SOURCES[kind]
    df = _read_csv(path) if fmt == "csv" else _read_jsonl(path)
    if df is None:
        return jsonify({"success": False, "error": f"Berkas {os.path.relpath(path, BASE_DIR)} belum ada. Jalankan pipeline atau periksa folder data."}), 404
    return jsonify({"success": True, "columns": list(df.columns), "rows": _records(df.head(limit)), "total": int(len(df)),
                    "file": os.path.relpath(path, BASE_DIR).replace(os.sep, "/")})


@app.route("/run_pipeline", methods=["POST"])
def run_pipeline():
    """Menjalankan `python main.py` (interpreter yang sama dengan Flask / venv) secara sinkron."""
    if not RUN_LOCK.acquire(blocking=False):
        flash("Pipeline sedang berjalan. Tunggu hingga selesai sebelum menjalankan ulang.", "warning")
        return redirect(url_for("index") + "#tab-quality")
    started = time.time()
    target = "#tab-analytics"
    try:
        if not os.path.exists(MAIN_PY):
            flash(f"File main.py tidak ditemukan di {BASE_DIR}. Letakkan app.py dan main.py di folder yang sama.", "danger")
            return redirect(url_for("index") + "#tab-quality")
        missing = [os.path.relpath(p_, BASE_DIR) for p_ in (RAW_CSV, RAW_JSON) if not os.path.exists(p_)]
        if missing:
            flash("Dataset input tidak ditemukan: " + ", ".join(missing) + ". Letakkan kedua berkas di folder data/raw/.", "danger")
            return redirect(url_for("index") + "#tab-quality")

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"

        proc = subprocess.run([sys.executable, MAIN_PY], cwd=BASE_DIR, env=env, capture_output=True, text=True,
                              encoding="utf-8", errors="replace", timeout=PIPELINE_TIMEOUT_S)
        duration = time.time() - started
        with open(RUN_LOG, "w", encoding="utf-8") as fh:
            fh.write(proc.stdout or "")
            if proc.stderr:
                fh.write("\n----- STDERR -----\n" + proc.stderr)
        ok = proc.returncode == 0
        with open(RUN_INFO_JSON, "w", encoding="utf-8") as fh:
            json.dump({"finished_at": datetime.now().isoformat(timespec="seconds"),
                       "duration_s": round(duration, 1), "returncode": proc.returncode, "ok": ok}, fh)

        if ok:
            flash(f"Pipeline PySpark selesai dalam {duration:.1f} detik. Dashboard telah diperbarui.", "success")
        else:
            err_lines = [ln for ln in (proc.stderr or proc.stdout or "").splitlines() if ln.strip()]
            hint = next((ln for ln in reversed(err_lines) if "Error" in ln or "Exception" in ln),
                        err_lines[-1] if err_lines else "tanpa pesan error")
            flash(f"Pipeline gagal (exit code {proc.returncode}): {hint[:300]}. Lihat Pipeline console di bawah.", "danger")
            target = "#tab-quality"
    except subprocess.TimeoutExpired:
        flash(f"Pipeline melebihi batas waktu {PIPELINE_TIMEOUT_S} detik dan dihentikan.", "danger")
        target = "#tab-quality"
    except Exception as exc:
        app.logger.exception("run_pipeline gagal")
        flash(f"Tidak dapat menjalankan pipeline: {exc}", "danger")
        target = "#tab-quality"
    finally:
        RUN_LOCK.release()
    return redirect(url_for("index") + target)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False, threaded=True)