#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IFB-451 BIG DATA - ETS | Healthcare Insurance Claims Analytics (PySpark)

STRUKTUR FOLDER
  data/raw/Dataset_HealthcareInsurance.csv     <- INPUT klaim (terstruktur, 942 baris mentah)
  data/raw/Dataset_HealthcareInsurance.json    <- INPUT profil pasien (JSON Lines, semi-terstruktur)
  data/processed/                              <- OUTPUT data bersih (ditulis otomatis saat pipeline berjalan)
      claims_clean.csv  claims_clean.parquet/  claims_quarantine.csv  patients_clean.json
  output/                                      <- OUTPUT analitik untuk dashboard Flask
      flagged_claims.csv  summary_cpt_risk.csv  provider_totals.csv  pipeline_metrics.json

ALUR PIPELINE
  [0] Variety   : baca CSV (skema eksplisit) + JSON (skema eksplisit), tampilkan skema keduanya
  [1] Velocity  : simulasi pengajuan klaim periodik (batch per kuartal) + ukur ingestion latency
  [2] Veracity  : profil kualitas data MENTAH (cacat asli) -> pembersihan -> ukuran before/after -> simpan ke data/processed
  [3] Volume    : scale-up SIMULASI (replikasi klaim & pasien) untuk benchmark komputasi
  [4] Operasi A : agregasi sederhana total klaim per provider
  [5] Operasi B : join CSV+JSON, window function, deteksi pola klaim tidak wajar
  [6] Output    : tabel perbandingan waktu A vs B + file hasil untuk dashboard

Jalankan:  python main.py
           (opsional) python main.py --raw-dir data/raw --processed-dir data/processed --out output --scale 1000

CATATAN KEJUJURAN DATA (wajib disebut di video/PDF):
  * Cacat data pada tahap [2] adalah cacat ASLI dari file mentah (bukan simulasi): klaim ganda,
    spesialisasi provider kosong, nilai bayar negatif, dan PATIENT_STATE kosong di seluruh JSON.
  * Dataset tidak punya kolom diagnosis. Kode diagnosis digantikan CPT_CODE (kode prosedur).
  * RISK_TIER tidak ada di data; DITURUNKAN di Spark dari total pembayaran per pasien (tertile). Bukan data klinis.
  * Dataset kecil. Skala 1000x di tahap [3] adalah SIMULASI replikasi, hanya untuk benchmark waktu.
"""
import argparse
import json
import os
import statistics
import time
from datetime import datetime
from functools import reduce
from operator import or_

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, IntegerType, LongType, StringType,
                               StructField, StructType)

# --- KONFIGURASI HADOOP WINDOWS ---
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

# ----------------------------------------------------------------------------
# KONFIGURASI
# ----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_CSV_NAME = "Dataset_HealthcareInsurance.csv"
RAW_JSON_NAME = "Dataset_HealthcareInsurance.json"

SCALE_FACTOR = 1000       # replikasi klaim & pasien x1000 (SIMULASI volume)
Z_THRESHOLD = 2.0         # z-score dalam satu CPT
RATIO_THRESHOLD = 5.0     # paid >= 5x median CPT
RARE_SPECIALTY_SHARE = 0.15   # spesialisasi < 15% klaim suatu CPT dianggap tidak lazim
BURST_30D = 10            # >= 10 klaim pasien dalam 30 hari
N_TIMING_RUNS = 3         # median dari 3 kali jalan (setelah warm-up)

CLAIM_COLS = ["CLAIM_ID", "PATIENT_ID", "DATE_OF_SERVICE", "CPT_CODE", "CPT_DESCRIPTION",
              "PROVIDER_NPI", "PROVIDER_SPECIALTY", "TOTAL_PAID_BY_INSURANCE", "PATIENT_COPAYMENT"]
CRITICAL = ["CLAIM_ID", "PATIENT_ID", "DATE_OF_SERVICE", "CPT_CODE", "PROVIDER_NPI",
            "TOTAL_PAID_BY_INSURANCE", "PATIENT_COPAYMENT"]          # PROVIDER_SPECIALTY ditangani terpisah (bisa dipulihkan)

CLAIM_SCHEMA = StructType([
    StructField("CLAIM_ID", StringType(), True),
    StructField("PATIENT_ID", StringType(), True),
    StructField("DATE_OF_SERVICE", DateType(), True),
    StructField("CPT_CODE", IntegerType(), True),
    StructField("CPT_DESCRIPTION", StringType(), True),
    StructField("PROVIDER_NPI", LongType(), True),
    StructField("PROVIDER_SPECIALTY", StringType(), True),
    StructField("TOTAL_PAID_BY_INSURANCE", IntegerType(), True),
    StructField("PATIENT_COPAYMENT", IntegerType(), True),
])
PATIENT_SCHEMA = StructType([                      # JSON datar; PATIENT_STATE kosong di seluruh baris
    StructField("PATIENT_ID", StringType(), True),
    StructField("PATIENT_STATE", StringType(), True),
    StructField("PRODUCT_LINE", StringType(), True),
])


def banner(title):
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78)


def timed(label, fn, runs=N_TIMING_RUNS, warmup=True):
    """Ukur latensi (detik) dengan time.time(). fn harus memicu action Spark.
    Warm-up 1x (JIT/JVM + cache plan) tidak dihitung; hasil = median dari `runs`."""
    if warmup:
        fn()
    samples = []
    result = None
    for _ in range(runs):
        t0 = time.time()
        result = fn()
        samples.append(time.time() - t0)
    med = statistics.median(samples)
    print(f"[TIMER] {label:<46s} median={med:8.3f}s  runs={[round(s, 3) for s in samples]}")
    return result, med


# ----------------------------------------------------------------------------
# [0] VARIETY - baca CSV (terstruktur) + JSON (semi-terstruktur) dari data/raw
# ----------------------------------------------------------------------------
def load_raw(spark, raw_dir):
    banner("[0] VARIETY: membaca data/raw (CSV klaim + JSON profil pasien)")
    csv_path = os.path.join(raw_dir, RAW_CSV_NAME)
    json_path = os.path.join(raw_dir, RAW_JSON_NAME)
    for p in (csv_path, json_path):
        if not os.path.exists(p):
            raise FileNotFoundError(f"File input tidak ditemukan: {p}\n"
                                    f"Letakkan {RAW_CSV_NAME} dan {RAW_JSON_NAME} di folder {raw_dir}")
    claims = spark.read.csv(csv_path, header=True, schema=CLAIM_SCHEMA)
    patients = spark.read.json(json_path, schema=PATIENT_SCHEMA)       # JSON Lines, 1 objek per baris
    print(f"CSV : {csv_path}  ({os.path.getsize(csv_path)/1024:.1f} KB, {claims.count()} baris, {len(claims.columns)} kolom)")
    print(f"JSON: {json_path} ({os.path.getsize(json_path)/1024:.1f} KB, {patients.count()} objek, {len(patients.columns)} field)")
    print("\nSkema CSV (terstruktur, tipe tetap):")
    claims.printSchema()
    print("Skema JSON (semi-terstruktur, JSON Lines):")
    patients.printSchema()
    return csv_path, json_path, claims, patients


# ----------------------------------------------------------------------------
# [1] VELOCITY - simulasi pengajuan klaim periodik (batch per kuartal)
# ----------------------------------------------------------------------------
def simulate_batches(spark, csv_path, out_dir):
    banner("[1] VELOCITY (SIMULASI): pengajuan klaim periodik per kuartal")
    full = spark.read.csv(csv_path, header=True, schema=CLAIM_SCHEMA)
    full = full.withColumn("batch", F.concat(F.year("DATE_OF_SERVICE").cast("string"),
                                             F.lit("Q"), F.quarter("DATE_OF_SERVICE").cast("string")))
    batch_ids = sorted(r["batch"] for r in full.select("batch").distinct().collect())
    batch_dir = os.path.join(out_dir, "incoming_batches")
    for b in batch_ids:                                   # tulis tiap batch = "file kiriman provider"
        (full.filter(F.col("batch") == b).drop("batch").coalesce(1)
             .write.mode("overwrite").option("header", True).csv(os.path.join(batch_dir, b)))

    acc, rows_total, log = None, 0, []
    wall0 = time.time()
    for b in batch_ids:                                   # penerimaan berurutan = klaim "tiba" periodik
        arrival = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        t0 = time.time()
        df = spark.read.csv(os.path.join(batch_dir, b), header=True, schema=CLAIM_SCHEMA)
        n = df.count()                                    # action -> ingestion benar-benar terjadi
        acc = df if acc is None else acc.unionByName(df)
        rows_total += n
        latency = time.time() - t0
        log.append((b, arrival, n, latency))
        print(f"  batch {b} | arrival={arrival} | rows={n:4d} | ingest latency={latency:6.3f}s "
              f"| throughput={n/latency:8.1f} rows/s")
    wall = time.time() - wall0
    lat = [x[3] for x in log]
    print(f"  TOTAL {rows_total} baris dalam {len(batch_ids)} batch, wall={wall:.3f}s | "
          f"latency rata-rata={statistics.mean(lat):.3f}s | max={max(lat):.3f}s")
    return acc, log


# ----------------------------------------------------------------------------
# [2] VERACITY - profil kualitas (cacat ASLI), pembersihan, penyimpanan ke data/processed
# ----------------------------------------------------------------------------
def flag_issues(df):
    """Tambahkan kolom penanda masalah kualitas per baris (tanpa mengubah/menghapus data)."""
    w = Window.partitionBy("CLAIM_ID").orderBy("DATE_OF_SERVICE")
    null_crit = reduce(or_, [F.col(c).isNull() for c in CRITICAL])
    return (df.withColumn("_rn", F.row_number().over(w))
              .withColumn("is_duplicate", F.col("_rn") > 1)                       # salinan ke-2 dst. dari CLAIM_ID yang sama
              .withColumn("null_specialty", F.col("PROVIDER_SPECIALTY").isNull())
              .withColumn("null_critical", null_crit)
              .withColumn("nonpositive_paid", F.col("TOTAL_PAID_BY_INSURANCE") <= 0)
              .withColumn("copay_gt_paid", F.col("PATIENT_COPAYMENT") > F.col("TOTAL_PAID_BY_INSURANCE"))
              .drop("_rn"))


def conflicting_duplicates(df):
    """Jumlah CLAIM_ID yang punya >1 versi isi berbeda (duplikat TIDAK identik). 0 = semua duplikat identik."""
    others = [c for c in CLAIM_COLS if c != "CLAIM_ID"]
    return (df.groupBy("CLAIM_ID").agg(F.size(F.collect_set(F.struct(*others))).alias("v"))
              .filter(F.col("v") > 1).count())


def quality_report(df, title):
    """Ukuran kualitas yang dapat dibandingkan before/after. issue_rows = baris DISTINCT yang punya >=1 masalah."""
    f = flag_issues(df)
    b = lambda c: F.coalesce(F.col(c), F.lit(False))
    issue = b("is_duplicate") | b("null_specialty") | b("null_critical") | b("nonpositive_paid") | b("copay_gt_paid")
    r = f.agg(F.count("*").alias("total_rows"),
              F.sum(b("is_duplicate").cast("int")).alias("duplicate_claim_id"),
              F.sum(b("null_specialty").cast("int")).alias("null_specialty"),
              F.sum(b("null_critical").cast("int")).alias("rows_with_null_critical"),
              F.sum(b("nonpositive_paid").cast("int")).alias("nonpositive_paid"),
              F.sum(b("copay_gt_paid").cast("int")).alias("copay_gt_paid"),
              F.sum(issue.cast("int")).alias("issue_rows")).first().asDict()
    q = {k: int(v or 0) for k, v in r.items()}
    q["issue_rows_pct"] = round(100.0 * q["issue_rows"] / max(q["total_rows"], 1), 2)
    print(f"\n  [{title}]")
    for k, v in q.items():
        print(f"    {k:<28s}: {v}")
    return q


def clean_claims(df):
    """Aturan penanganan (terdokumentasi, dapat dipertanggungjawabkan):
       R1 hapus salinan duplikat CLAIM_ID (pertahankan 1; dicatat di karantina dengan alasan DUPLICATE_CLAIM_ID)
       R2 pulihkan PROVIDER_SPECIALTY kosong lewat pemetaan PROVIDER_NPI -> spesialisasi, HANYA jika NPI itu
          memiliki tepat satu spesialisasi di data (ditandai kolom specialty_imputed)
       R3 karantina baris dengan nilai kosong yang tidak bisa dipulihkan di kolom kritis (tidak ditebak)
       R4 karantina TOTAL_PAID <= 0 (nilai uang tidak boleh diperbaiki dengan menebak tanda atau angkanya)
       R5 copay > paid TIDAK dihapus: hanya diberi flag billing_inconsistency untuk ditinjau manual."""
    f = flag_issues(df)
    dups = f.filter(F.col("is_duplicate")).select(*CLAIM_COLS).withColumn("quarantine_reason", F.lit("DUPLICATE_CLAIM_ID"))
    d1 = f.filter(~F.col("is_duplicate")).select(*CLAIM_COLS)

    lookup = (d1.filter(F.col("PROVIDER_SPECIALTY").isNotNull())
                .groupBy("PROVIDER_NPI").agg(F.collect_set("PROVIDER_SPECIALTY").alias("_s"))
                .filter(F.size("_s") == 1).select("PROVIDER_NPI", F.col("_s")[0].alias("_spec_lookup")))
    d2 = (d1.join(F.broadcast(lookup), on="PROVIDER_NPI", how="left")
            .withColumn("specialty_imputed", F.col("PROVIDER_SPECIALTY").isNull() & F.col("_spec_lookup").isNotNull())
            .withColumn("PROVIDER_SPECIALTY", F.coalesce(F.col("PROVIDER_SPECIALTY"), F.col("_spec_lookup")))
            .drop("_spec_lookup"))

    null_crit = reduce(or_, [F.col(c).isNull() for c in CRITICAL]) | F.col("PROVIDER_SPECIALTY").isNull()
    nonpos = F.coalesce(F.col("TOTAL_PAID_BY_INSURANCE") <= 0, F.lit(False))
    reason = F.when(null_crit, F.lit("NULL_UNRECOVERABLE")).when(nonpos, F.lit("NONPOSITIVE_PAID"))
    d2 = d2.withColumn("quarantine_reason", reason)

    good = (d2.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
              .withColumn("billing_inconsistency", F.col("PATIENT_COPAYMENT") > F.col("TOTAL_PAID_BY_INSURANCE"))
              .select(*CLAIM_COLS, "specialty_imputed", "billing_inconsistency"))
    bad = d2.filter(F.col("quarantine_reason").isNotNull()).select(*CLAIM_COLS, "quarantine_reason")
    return good, dups.unionByName(bad)


def build_patients(patients_raw, clean):
    """Profil pasien bersih: PATIENT_STATE kosong -> 'UNKNOWN'; tambah atribut TURUNAN dari klaim bersih
    (CLAIM_COUNT, TOTAL_PAID) dan RISK_TIER (tertile total pembayaran, bukan data klinis)."""
    prof = (clean.groupBy("PATIENT_ID")
                 .agg(F.count("*").alias("CLAIM_COUNT"), F.sum("TOTAL_PAID_BY_INSURANCE").alias("TOTAL_PAID")))
    tier = F.ntile(3).over(Window.orderBy("TOTAL_PAID", "PATIENT_ID"))
    prof = prof.withColumn("_t", tier).withColumn(
        "RISK_TIER", F.when(F.col("_t") == 1, "LOW").when(F.col("_t") == 2, "MEDIUM").otherwise("HIGH")).drop("_t")
    pat = (patients_raw.dropDuplicates(["PATIENT_ID"])
                       .withColumn("PATIENT_STATE", F.coalesce(F.col("PATIENT_STATE"), F.lit("UNKNOWN")))
                       .join(prof, on="PATIENT_ID", how="left")
                       .withColumn("RISK_TIER", F.coalesce(F.col("RISK_TIER"), F.lit("NO_CLAIMS")))
                       .select("PATIENT_ID", "PATIENT_STATE", "PRODUCT_LINE", "CLAIM_COUNT", "TOTAL_PAID", "RISK_TIER"))
    return pat


def save_processed(processed_dir, clean, quarantine, patients):
    """Simpan dataset bersih ke data/processed (CSV tunggal agar mudah dibuka, + Parquet untuk Spark)."""
    banner(f"[2b] PENYIMPANAN: dataset bersih -> {processed_dir}")
    os.makedirs(processed_dir, exist_ok=True)
    files = []

    def _csv(df, name, order):
        path = os.path.join(processed_dir, name)
        out = df.withColumn("DATE_OF_SERVICE", F.date_format("DATE_OF_SERVICE", "yyyy-MM-dd")).orderBy(order).toPandas()
        out.to_csv(path, index=False)
        files.append({"file": f"data/processed/{name}", "rows": int(len(out)), "kb": round(os.path.getsize(path) / 1024, 1),
                      "desc": "klaim bersih" if "clean" in name else "klaim dikarantina (beserta alasan)"})
        print(f"  Ditulis: {path}  ({len(out)} baris)")

    _csv(clean, "claims_clean.csv", "CLAIM_ID")
    _csv(quarantine, "claims_quarantine.csv", "CLAIM_ID")

    ppath = os.path.join(processed_dir, "patients_clean.json")
    pp = patients.orderBy("PATIENT_ID").toPandas()
    pp.to_json(ppath, orient="records", lines=True, force_ascii=False)
    files.append({"file": "data/processed/patients_clean.json", "rows": int(len(pp)), "kb": round(os.path.getsize(ppath) / 1024, 1),
                  "desc": "profil pasien bersih + RISK_TIER turunan"})
    print(f"  Ditulis: {ppath} ({len(pp)} baris)")

    parquet_path = os.path.join(processed_dir, "claims_clean.parquet")
    clean.write.mode("overwrite").parquet(parquet_path)
    files.append({"file": "data/processed/claims_clean.parquet/", "rows": int(clean.count()), "kb": None,
                  "desc": "klaim bersih (format kolumnar Spark)"})
    print(f"  Ditulis: {parquet_path} (Parquet)")
    return files


# ----------------------------------------------------------------------------
# [3] VOLUME (SIMULASI) - replikasi untuk benchmark
# ----------------------------------------------------------------------------
def scale_up(spark, claims, patients, factor):
    ks = spark.range(factor).withColumnRenamed("id", "k")
    sfx = lambda c: F.concat(F.col(c), F.lit("-"), F.col("k").cast("string"))
    big_claims = (claims.crossJoin(ks)
                  .withColumn("CLAIM_ID", sfx("CLAIM_ID"))
                  .withColumn("PATIENT_ID", sfx("PATIENT_ID")).drop("k"))
    big_pat = (patients.crossJoin(ks)
               .withColumn("PATIENT_ID", sfx("PATIENT_ID")).drop("k"))
    return big_claims, big_pat


# ----------------------------------------------------------------------------
# [4] OPERASI A - agregasi sederhana
# ----------------------------------------------------------------------------
def operation_a(claims):
    return (claims.groupBy("PROVIDER_NPI", "PROVIDER_SPECIALTY")
            .agg(F.count("*").alias("n_claims"),
                 F.sum("TOTAL_PAID_BY_INSURANCE").alias("total_paid"),
                 F.round(F.avg("TOTAL_PAID_BY_INSURANCE"), 1).alias("avg_paid"))
            .orderBy(F.desc("total_paid")))


# ----------------------------------------------------------------------------
# [5] OPERASI B - join CSV+JSON, window function, deteksi pola tidak wajar
# ----------------------------------------------------------------------------
def operation_b(claims, patients):
    # (1) JOIN heterogen: CSV (klaim) x JSON (profil pasien: lini produk + risk tier turunan)
    pat = patients.select("PATIENT_ID",
                          F.col("PRODUCT_LINE").alias("product_line"),
                          F.col("RISK_TIER").alias("risk_tier"))
    j = claims.join(pat, on="PATIENT_ID", how="inner")

    # (2) Statistik harga per CPT: mean/std (agregasi) + median robust (percentile_approx), di-join balik
    cpt_stats = (j.groupBy("CPT_CODE")
                  .agg(F.avg("TOTAL_PAID_BY_INSURANCE").alias("cpt_mean"),
                       F.stddev("TOTAL_PAID_BY_INSURANCE").alias("cpt_std"),
                       F.percentile_approx("TOTAL_PAID_BY_INSURANCE", 0.5).alias("cpt_median")))
    w_cpt = Window.partitionBy("CPT_CODE")
    j = (j.join(cpt_stats, on="CPT_CODE", how="left")
          .withColumn("z_score", (F.col("TOTAL_PAID_BY_INSURANCE") - F.col("cpt_mean")) /
                                 F.when(F.col("cpt_std") == 0, None).otherwise(F.col("cpt_std")))
          .withColumn("ratio_to_median", F.col("TOTAL_PAID_BY_INSURANCE") / F.col("cpt_median")))

    # (3) WINDOW: kesesuaian spesialisasi provider terhadap prosedur (share spesialisasi per CPT)
    w_spec = Window.partitionBy("CPT_CODE", "PROVIDER_SPECIALTY")
    j = (j.withColumn("spec_cnt", F.count("*").over(w_spec))
          .withColumn("cpt_cnt", F.count("*").over(w_cpt))
          .withColumn("specialty_share", F.col("spec_cnt") / F.col("cpt_cnt")))

    # (4) WINDOW temporal: jumlah klaim pasien dalam 30 hari terakhir (range frame berbasis detik)
    day = 86400
    w_30d = (Window.partitionBy("PATIENT_ID")
                   .orderBy(F.col("DATE_OF_SERVICE").cast("timestamp").cast("long"))
                   .rangeBetween(-30 * day, 0))
    j = j.withColumn("claims_last_30d", F.count("*").over(w_30d))

    # (5) Flag anomali (4 aturan + inkonsistensi tagihan)
    nf = lambda cond: F.coalesce(cond, F.lit(False))        # null -> False
    j = (j.withColumn("flag_zscore", nf(F.col("z_score") >= Z_THRESHOLD))
          .withColumn("flag_price_ratio", nf(F.col("ratio_to_median") >= RATIO_THRESHOLD))
          .withColumn("flag_rare_specialty", nf(F.col("specialty_share") < RARE_SPECIALTY_SHARE))
          .withColumn("flag_burst_30d", nf(F.col("claims_last_30d") >= BURST_30D))
          .withColumn("flag_copay_gt_paid", nf(F.col("PATIENT_COPAYMENT") > F.col("TOTAL_PAID_BY_INSURANCE"))))
    rules = ["flag_zscore", "flag_price_ratio", "flag_rare_specialty", "flag_burst_30d", "flag_copay_gt_paid"]
    j = j.withColumn("n_flags", sum(F.col(r).cast("int") for r in rules))
    return j


def summarize_b(j):
    """Ringkasan per Kode CPT x Tingkat Risiko (sesuai spesifikasi tugas)."""
    return (j.groupBy("CPT_CODE", "CPT_DESCRIPTION", "risk_tier")
             .agg(F.count("*").alias("n_claims"),
                  F.sum("TOTAL_PAID_BY_INSURANCE").alias("total_paid"),
                  F.sum((F.col("n_flags") >= 1).cast("int")).alias("n_flagged"),
                  F.sum(F.when(F.col("n_flags") >= 1, F.col("TOTAL_PAID_BY_INSURANCE")).otherwise(0)).alias("flagged_paid"))
             .orderBy(F.desc("flagged_paid")))


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", default=os.path.join(BASE_DIR, "data", "raw"))
    ap.add_argument("--processed-dir", default=os.path.join(BASE_DIR, "data", "processed"))
    ap.add_argument("--out", default=os.path.join(BASE_DIR, "output"))
    ap.add_argument("--scale", type=int, default=SCALE_FACTOR)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    spark = (SparkSession.builder.appName("IFB451-Claims-Analytics").master("local[*]")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark {spark.version} | cores={spark.sparkContext.defaultParallelism}")
    T = {}
    M = {"generated_at": datetime.now().isoformat(timespec="seconds"), "scale_factor": args.scale}  # metrik untuk dashboard

    # [0] Variety
    csv_path, json_path, claims_raw, patients_raw = load_raw(spark, args.raw_dir)

    # [1] Velocity
    ingested, batch_log = simulate_batches(spark, csv_path, args.out)
    ingested = ingested.cache()
    M["velocity"] = {"batches": [{"batch": b, "arrival": a, "rows": int(n), "latency_s": round(float(l), 4)}
                                 for (b, a, n, l) in batch_log]}

    # [2] Veracity (cacat ASLI dari data mentah)
    banner("[2] VERACITY: profil kualitas data mentah -> pembersihan -> before/after")
    q_before = quality_report(ingested, "SEBELUM pembersihan (data mentah)")
    n_conflict = conflicting_duplicates(ingested)
    p_total = patients_raw.count()
    p_state_null = patients_raw.filter(F.col("PATIENT_STATE").isNull()).count()
    print(f"\n  Duplikat dengan isi BERBEDA (konflik): {n_conflict}  (0 = semua duplikat identik, aman dihapus)")
    print(f"  JSON pasien: PATIENT_STATE kosong pada {p_state_null} dari {p_total} pasien "
          f"-> diisi 'UNKNOWN', kolom ini tidak dipakai di analisis")

    t0 = time.time()
    clean, quarantine = clean_claims(ingested)
    clean = clean.cache(); n_clean = clean.count()
    quarantine = quarantine.cache(); n_quar = quarantine.count()
    patients = build_patients(patients_raw, clean).cache(); patients.count()
    T["cleaning_latency"] = time.time() - t0

    q_after = quality_report(clean, "SESUDAH pembersihan")
    reasons = {r["quarantine_reason"]: int(r["count"]) for r in quarantine.groupBy("quarantine_reason").count().collect()}
    n_imputed = clean.filter(F.col("specialty_imputed")).count()
    print(f"\n  Bersih: {n_clean} baris | Karantina: {n_quar} baris {reasons} | spesialisasi dipulihkan: {n_imputed}")
    print(f"  Waktu pembersihan: {T['cleaning_latency']:.3f}s")
    print(f"  Baris bermasalah: {q_before['issue_rows_pct']}% -> {q_after['issue_rows_pct']}% "
          f"(sisa = klaim copay>paid yang dipertahankan & di-flag)")
    M["veracity"] = {"simulated": False, "before": q_before, "after": q_after,
                     "quarantined_rows": int(n_quar), "quarantine_reasons": reasons, "clean_rows": int(n_clean),
                     "duplicates_removed": reasons.get("DUPLICATE_CLAIM_ID", 0),
                     "specialty_imputed": int(n_imputed), "conflicting_duplicates": int(n_conflict),
                     "patients_total": int(p_total), "patients_state_null": int(p_state_null),
                     "cleaning_latency_s": round(float(T["cleaning_latency"]), 4)}
    M["processed_files"] = save_processed(args.processed_dir, clean, quarantine, patients)

    clean_real = clean          # data bersih ini yang dipakai Operasi A/B dan semua temuan

    # [3] Volume SIMULASI
    banner(f"[3] VOLUME (SIMULASI): replikasi x{args.scale}")
    big_claims, big_pat = scale_up(spark, clean_real, patients, args.scale)
    big_claims = big_claims.cache(); n_big = big_claims.count()
    big_pat = big_pat.cache(); n_bigp = big_pat.count()
    print(f"  Mentah: {q_before['total_rows']:,} baris | Bersih: {clean_real.count():,} klaim | {patients.count():,} pasien")
    print(f"  Skala : {n_big:,} klaim | {n_bigp:,} pasien | partisi={big_claims.rdd.getNumPartitions()}")
    M["volume"] = {"rows_raw": int(q_before["total_rows"]), "rows_real": int(clean_real.count()), "rows_scaled": int(n_big),
                   "patients_real": int(patients.count()), "patients_scaled": int(n_bigp),
                   "partitions_scaled": int(big_claims.rdd.getNumPartitions())}

    # [4][5] Benchmark A vs B pada skala asli dan skala simulasi
    banner("[4][5] BENCHMARK: Operasi A (agregasi) vs Operasi B (relational + temporal)")
    results = {}
    for tag, c, p in (("ASLI", clean_real, patients), ("SKALA", big_claims, big_pat)):
        _, ta = timed(f"Operasi A  [{tag}]", lambda: operation_a(c).collect())
        _, tb = timed(f"Operasi B  [{tag}]", lambda: summarize_b(operation_b(c, p)).collect())
        results[tag] = (ta, tb)

    # Rencana eksekusi: jumlah Exchange (shuffle) = penyebab utama biaya Operasi B
    def n_exchange(df):
        plan = df._jdf.queryExecution().executedPlan().toString()
        return plan.count("Exchange")
    ex_a = n_exchange(operation_a(clean_real))
    ex_b = n_exchange(summarize_b(operation_b(clean_real, patients)))
    print(f"\n  Shuffle (Exchange) pada physical plan -> Operasi A: {ex_a} | Operasi B: {ex_b}")

    print("\n" + "-" * 78)
    print(f"{'Skala':<8}{'Baris':>12}{'Op A (s)':>12}{'Op B (s)':>12}{'B/A':>8}")
    n_real = clean_real.count()
    for tag, n in (("ASLI", n_real), ("SKALA", n_big)):
        ta, tb = results[tag]
        print(f"{tag:<8}{n:>12,}{ta:>12.3f}{tb:>12.3f}{tb/ta:>8.1f}x")
    print("-" * 78)
    M["benchmark"] = {
        "ASLI":  {"rows": int(n_real), "op_a_s": round(results["ASLI"][0], 4),  "op_b_s": round(results["ASLI"][1], 4)},
        "SKALA": {"rows": int(n_big),  "op_a_s": round(results["SKALA"][0], 4), "op_b_s": round(results["SKALA"][1], 4)},
        "exchange_a": int(ex_a), "exchange_b": int(ex_b), "timing_runs": N_TIMING_RUNS}

    # Hasil analitik (DATA ASLI BERSIH) -------------------------------------
    banner("[6] HASIL ANALITIK (data asli bersih)")
    print("\nOperasi A - total klaim per provider:")
    operation_a(clean_real).show(truncate=False)
    operation_a(clean_real).toPandas().to_csv(os.path.join(args.out, "provider_totals.csv"), index=False)   # untuk chart dashboard

    jb = operation_b(clean_real, patients).cache()
    flagged = jb.filter(F.col("n_flags") >= 1)
    n_flag = flagged.count()
    paid_all = jb.agg(F.sum("TOTAL_PAID_BY_INSURANCE")).first()[0]
    paid_flag = flagged.agg(F.sum("TOTAL_PAID_BY_INSURANCE")).first()[0]
    print(f"Klaim ter-flag: {n_flag} dari {jb.count()} ({100*n_flag/jb.count():.1f}%) | "
          f"nilai ter-flag ${paid_flag:,} dari ${paid_all:,} ({100*paid_flag/paid_all:.1f}%)")

    M["value"] = {"total_claims": int(jb.count()), "flagged_claims": int(n_flag),
                  "paid_total": int(paid_all), "paid_flagged": int(paid_flag)}
    print("\nRingkasan per CPT x risk_tier (urut nilai ter-flag):")
    summarize_b(jb).show(15, truncate=False)

    print("Penggantian sendi (CPT 27132 hip / 27447 knee) per provider:")
    (jb.filter(F.col("CPT_CODE").isin(27132, 27447))
       .groupBy("PROVIDER_NPI", "PROVIDER_SPECIALTY")
       .agg(F.count("*").alias("n"), F.round(F.avg("TOTAL_PAID_BY_INSURANCE"), 0).alias("avg_paid"),
            F.max("TOTAL_PAID_BY_INSURANCE").alias("max_paid"))
       .orderBy(F.desc("avg_paid")).show(truncate=False))

    print("Kontribusi tiap aturan flag:")
    for r in ["flag_zscore", "flag_price_ratio", "flag_rare_specialty", "flag_burst_30d", "flag_copay_gt_paid"]:
        _cnt = jb.filter(F.col(r)).count()
        M["value"].setdefault("rules", {})[r] = int(_cnt)
        print(f"  {r:<22s}: {_cnt}")

    print("\nTop 15 klaim paling mencurigakan:")
    (flagged.select("CLAIM_ID", "PATIENT_ID", "risk_tier", "CPT_DESCRIPTION", "PROVIDER_NPI",
                    "PROVIDER_SPECIALTY", "TOTAL_PAID_BY_INSURANCE", "ratio_to_median", "n_flags")
            .orderBy(F.desc("n_flags"), F.desc("TOTAL_PAID_BY_INSURANCE")).show(15, truncate=False))

    # Output untuk dashboard (Plotly / Tableau / Power BI)
    out_csv = os.path.join(args.out, "flagged_claims.csv")
    (flagged.withColumn("DATE_OF_SERVICE", F.date_format("DATE_OF_SERVICE", "yyyy-MM-dd"))
            .select("CLAIM_ID", "PATIENT_ID", "product_line", "risk_tier", "DATE_OF_SERVICE",
                    "CPT_CODE", "CPT_DESCRIPTION", "PROVIDER_NPI", "PROVIDER_SPECIALTY",
                    "TOTAL_PAID_BY_INSURANCE", "ratio_to_median", "z_score", "specialty_share",
                    "claims_last_30d", "n_flags")
            .toPandas().to_csv(out_csv, index=False))
    summarize_b(jb).toPandas().to_csv(os.path.join(args.out, "summary_cpt_risk.csv"), index=False)
    print(f"\nDisimpan: {out_csv} dan summary_cpt_risk.csv")
    joint = (jb.filter(F.col("CPT_CODE").isin(27132, 27447))
               .groupBy("PROVIDER_NPI", "PROVIDER_SPECIALTY")
               .agg(F.count("*").alias("n"), F.round(F.avg("TOTAL_PAID_BY_INSURANCE"), 0).alias("avg_paid"),
                    F.max("TOTAL_PAID_BY_INSURANCE").alias("max_paid"))
               .orderBy(F.desc("avg_paid")).toPandas())
    M["joint_replacement"] = joint.to_dict("records")
    metrics_path = os.path.join(args.out, "pipeline_metrics.json")
    with open(metrics_path, "w", encoding="utf-8") as fh:
        json.dump(M, fh, indent=2, ensure_ascii=False,
                  default=lambda o: o.item() if hasattr(o, "item") else str(o))
    print(f"Metrik pipeline disimpan: {metrics_path}")
    print("\nLIMITASI: 10 pasien & 7 provider, 2 tahun; tanpa diagnosis; PATIENT_STATE kosong; risk_tier turunan; "
          "flag = indikasi untuk audit, BUKAN bukti fraud.")
    spark.stop()


if __name__ == "__main__":
    main()