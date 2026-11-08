#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Claims Analytics Command Center - Flask backend
IFB-451 BIG DATA | Healthcare Insurance Claims Analytics

Struktur proyek yang diharapkan (sesuai folder repo):
    app.py                      <- file ini
    main.py                     <- pipeline PySpark (menulis data/processed dan output/)
    templates/index.html        <- frontend dashboard
    data/raw/Dataset_HealthcareInsurance.csv     <- input klaim
    data/raw/Dataset_HealthcareInsurance.json    <- input profil pasien
    data/processed/             <- dataset BERSIH hasil pipeline (claims_clean.csv, claims_quarantine.csv, ...)
    output/                     <- dibuat/diisi oleh main.py
        flagged_claims.csv
        summary_cpt_risk.csv
        pipeline_metrics.json   <- waktu eksekusi, kualitas data, volume (dipakai panel Velocity/Veracity/Volume)

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
from flask import Flask, flash, redirect, render_template, url_for

# ----------------------------------------------------------------------------
# KONFIGURASI
# ----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
RAW_INPUTS = [os.path.join(RAW_DIR, "Dataset_HealthcareInsurance.csv"),
              os.path.join(RAW_DIR, "Dataset_HealthcareInsurance.json")]
MAIN_PY = os.path.join(BASE_DIR, "main.py")
FLAGGED_CSV = os.path.join(OUTPUT_DIR, "flagged_claims.csv")
SUMMARY_CSV = os.path.join(OUTPUT_DIR, "summary_cpt_risk.csv")
METRICS_JSON = os.path.join(OUTPUT_DIR, "pipeline_metrics.json")
RUN_INFO_JSON = os.path.join(OUTPUT_DIR, "dashboard_last_run.json")
RUN_LOG = os.path.join(OUTPUT_DIR, "pipeline_last_run.log")

PIPELINE_TIMEOUT_S = int(os.environ.get("PIPELINE_TIMEOUT_S", "1800"))   # batas waktu eksekusi Spark (30 menit)
LOG_TAIL_LINES = 80

# Ambang aturan flag - HARUS sama dengan konstanta di main.py
Z_THRESHOLD = 2.0
RATIO_THRESHOLD = 5.0
RARE_SPECIALTY_SHARE = 0.15
BURST_30D = 10

# Nilai acuan statis (dipakai hanya bila output/pipeline_metrics.json belum ada)
STATIC_REFERENCE = {                      # profil dataset di data/raw (942 baris mentah -> 906 bersih)
    "rows_raw": 942,
    "rows_real": 906,
    "scale_factor": 1000,
    "patients_real": 10,
    "issue_pct_before": 6.16,
    "issue_pct_after": 0.11,
}

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "ifb451-claims-dashboard-local")
RUN_LOCK = threading.Lock()


# ----------------------------------------------------------------------------
# UTILITAS
# ----------------------------------------------------------------------------
def _py(v):
    """Ubah nilai pandas/numpy menjadi tipe Python yang aman untuk JSON (NaN -> None)."""
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
    out = []
    for rec in df.to_dict("records"):
        out.append({k: _py(v) for k, v in rec.items()})
    return out


def _read_csv(path):
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path)
    except Exception as exc:                                  # file korup / sedang ditulis
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
    """Daftar berkas di data/processed (dipakai bila pipeline_metrics.json belum ada)."""
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
    """Alasan flag per klaim, diturunkan dari kolom di flagged_claims.csv (aturan sama dengan main.py)."""
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
    if n_flags > len(reasons):                                # 5 aturan: sisanya = copay > bayar asuransi
        reasons.append("biaya pasien > bayar asuransi")
    return reasons


# ----------------------------------------------------------------------------
# PENYUSUN DATA DASHBOARD
# ----------------------------------------------------------------------------
def build_dashboard_data():
    flagged = _read_csv(FLAGGED_CSV)
    summary = _read_csv(SUMMARY_CSV)
    metrics = _read_json(METRICS_JSON)
    run_info = _read_json(RUN_INFO_JSON)

    has_output = flagged is not None and summary is not None
    data = {
        "has_output": has_output,
        "has_metrics": metrics is not None,
        "generated_at": (metrics or {}).get("generated_at"),
        "last_run": run_info,
    }

    # ---------------- VOLUME ----------------
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

    # ---------------- VELOCITY ----------------
    bench = (metrics or {}).get("benchmark")
    batches = ((metrics or {}).get("velocity") or {}).get("batches") or []
    data["velocity"] = {"benchmark": bench, "batches": batches} if (bench or batches) else None

    # ---------------- VERACITY ----------------
    ver = (metrics or {}).get("veracity")
    if ver:
        data["veracity"] = {
            "before_pct": ver["before"]["issue_rows_pct"],
            "after_pct": ver["after"]["issue_rows_pct"],
            "before": ver["before"], "after": ver["after"],
            "quarantined_rows": ver.get("quarantined_rows"),
            "quarantine_reasons": ver.get("quarantine_reasons"),
            "duplicates_removed": ver.get("duplicates_removed"),
            "specialty_imputed": ver.get("specialty_imputed"),
            "conflicting_duplicates": ver.get("conflicting_duplicates"),
            "patients_total": ver.get("patients_total"),
            "patients_state_null": ver.get("patients_state_null"),
            "cleaning_latency_s": ver.get("cleaning_latency_s"),
            "source": "pipeline",
        }
    else:
        data["veracity"] = {
            "before_pct": STATIC_REFERENCE["issue_pct_before"],
            "after_pct": STATIC_REFERENCE["issue_pct_after"],
            "before": None, "after": None,
            "quarantined_rows": None, "quarantine_reasons": None, "duplicates_removed": None,
            "specialty_imputed": None, "conflicting_duplicates": None,
            "patients_total": None, "patients_state_null": None, "cleaning_latency_s": None,
            "source": "static",
        }
    data["processed_files"] = (metrics or {}).get("processed_files") or _scan_processed()

    # ---------------- VALUE ----------------
    if has_output:
        total_claims = int(summary["n_claims"].sum())
        total_paid = int(summary["total_paid"].sum())
        n_flagged = int(len(flagged))
        paid_flagged = int(flagged["TOTAL_PAID_BY_INSURANCE"].sum())

        by_cpt = (summary.groupby(["CPT_CODE", "CPT_DESCRIPTION"], as_index=False)
                  .agg(n_claims=("n_claims", "sum"), n_flagged=("n_flagged", "sum"),
                       flagged_paid=("flagged_paid", "sum"))
                  .query("n_flagged > 0"))
        top_by_value = _records(by_cpt.sort_values(["flagged_paid", "n_flagged"], ascending=False).head(5))
        top_by_count = _records(by_cpt.sort_values(["n_flagged", "flagged_paid"], ascending=False).head(5))

        top = (flagged.sort_values(["n_flags", "TOTAL_PAID_BY_INSURANCE"], ascending=[False, False])
                      .head(10))
        top_claims = []
        for rank, rec in enumerate(_records(top), start=1):
            rec["rank"] = rank
            rec["reasons"] = _explain_flags(rec)
            top_claims.append(rec)

        data["value"] = {
            "total_claims": total_claims,
            "flagged_claims": n_flagged,
            "flagged_pct": round(100.0 * n_flagged / total_claims, 1) if total_claims else 0,
            "paid_total": total_paid,
            "paid_flagged": paid_flagged,
            "paid_flagged_pct": round(100.0 * paid_flagged / total_paid, 1) if total_paid else 0,
            "top_by_value": top_by_value,
            "top_by_count": top_by_count,
            "top_claims": top_claims,
            "rules": ((metrics or {}).get("value") or {}).get("rules"),
            "joint_replacement": (metrics or {}).get("joint_replacement"),
        }
    else:
        data["value"] = None

    # ---------------- STATIS: VARIETY & DATASET ----------------
    data["static"] = {
        "dataset": "ABC Healthcare Company Claims Dataset (2017-2018)",
        "thresholds": {"z": Z_THRESHOLD, "ratio": RATIO_THRESHOLD, "share": RARE_SPECIALTY_SHARE, "burst": BURST_30D},
    }
    return data


# ----------------------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------------------
@app.route("/")
def index():
    data = build_dashboard_data()
    return render_template("index.html", data=data, log_tail=_tail(RUN_LOG))


@app.route("/run_pipeline", methods=["POST"])
def run_pipeline():
    """Menjalankan `python main.py` (interpreter yang sama dengan Flask / venv) secara sinkron."""
    if not RUN_LOCK.acquire(blocking=False):
        flash("Pipeline sedang berjalan. Tunggu hingga selesai sebelum menjalankan ulang.", "warning")
        return redirect(url_for("index"))
    started = time.time()
    try:
        if not os.path.exists(MAIN_PY):
            flash(f"File main.py tidak ditemukan di {BASE_DIR}. Letakkan app.py dan main.py di folder yang sama.", "danger")
            return redirect(url_for("index"))

        missing = [os.path.relpath(p_, BASE_DIR) for p_ in RAW_INPUTS if not os.path.exists(p_)]
        if missing:
            flash("Dataset input tidak ditemukan: " + ", ".join(missing) + ". Letakkan kedua berkas di folder data/raw/.", "danger")
            return redirect(url_for("index"))

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"       # console Windows (cp1252) tidak menolak karakter non-ASCII
        env["PYTHONUTF8"] = "1"

        proc = subprocess.run(
            [sys.executable, MAIN_PY], cwd=BASE_DIR, env=env,
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=PIPELINE_TIMEOUT_S,
        )
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

    except subprocess.TimeoutExpired:
        flash(f"Pipeline melebihi batas waktu {PIPELINE_TIMEOUT_S} detik dan dihentikan.", "danger")
    except Exception as exc:                      # kegagalan di luar proses (izin file, interpreter, dsb.)
        app.logger.exception("run_pipeline gagal")
        flash(f"Tidak dapat menjalankan pipeline: {exc}", "danger")
    finally:
        RUN_LOCK.release()
    return redirect(url_for("index"))


if __name__ == "__main__":
    # use_reloader=False: file di output/ berubah saat pipeline berjalan; jangan restart server.
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False, threaded=True)