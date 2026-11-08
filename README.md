# Healthcare Insurance Claims Analytics

Pipeline Big Data berbasis **PySpark** dan dashboard **Flask** untuk menganalisis klaim asuransi kesehatan,
dengan fokus mengidentifikasi pola klaim yang tidak wajar. Proyek ini dibuat untuk **ETS mata kuliah IFB-451 Big Data**,
Departemen Informatika, Institut Teknologi Nasional (Itenas) Bandung, tahun akademik 2026/2027.

---

## 1. Studi Kasus

**Healthcare Insurance Claims Analytics** (salah satu opsi studi kasus pada penugasan, Tabel 5.1).

Perusahaan asuransi membayar ribuan klaim dari berbagai provider. Pertanyaan yang ingin dijawab:

> Bagaimana 5V Big Data memengaruhi rancangan pipeline pemrosesan klaim asuransi kesehatan,
> dan klaim mana yang menunjukkan pola pembayaran yang layak ditinjau lebih lanjut?

Sebuah klaim dianggap **tidak wajar** bila menyimpang dari pola harga untuk kode prosedur yang sama, tidak sesuai
dengan spesialisasi provider yang menagihnya, atau terlalu sering diajukan oleh pasien yang sama dalam waktu singkat.
Hasilnya adalah **indikasi untuk audit, bukan bukti kecurangan (fraud)**.

### Dataset

Dua berkas di `data/raw/`:

| Berkas | Format | Isi |
|---|---|---|
| `Dataset_HealthcareInsurance.csv` | CSV (terstruktur) | 942 baris klaim mentah, 9 kolom: `CLAIM_ID`, `PATIENT_ID`, `DATE_OF_SERVICE`, `CPT_CODE`, `CPT_DESCRIPTION`, `PROVIDER_NPI`, `PROVIDER_SPECIALTY`, `TOTAL_PAID_BY_INSURANCE`, `PATIENT_COPAYMENT` |
| `Dataset_HealthcareInsurance.json` | JSON Lines (semi-terstruktur) | 10 objek pasien, 3 field: `PATIENT_ID`, `PATIENT_STATE`, `PRODUCT_LINE` |

Cakupan: 10 pasien, 7 provider (NPI), 6 spesialisasi, 14 kode CPT, Januari 2017 sampai Desember 2018.
Data bersifat ter-anonimisasi dan tidak memuat identitas pasien.

Sumber dataset: https://github.com/adiag321/Healthcare-Insurance-Claims-Analysis

### Hal yang perlu diketahui tentang data

- Dataset **tidak memiliki kolom diagnosis**. Kode prosedur `CPT_CODE` dipakai sebagai gantinya.
- `RISK_TIER` (LOW/MEDIUM/HIGH) **tidak ada di data**. Nilainya diturunkan di Spark dari total pembayaran per pasien
  (tertile). Ini atribut turunan, bukan data klinis, dan bersifat sirkular.
- `PATIENT_STATE` kosong pada seluruh pasien di JSON, sehingga tidak dipakai dalam analisis.
- Cacat data pada data mentah adalah **cacat asli**, bukan simulasi (lihat bagian Veracity).
- Yang **disimulasikan**: pengajuan klaim per kuartal (batch) dan skala data 1.000 kali untuk benchmark waktu.

---

## 2. Pemetaan 5V Big Data

| V | Yang didemonstrasikan | Di mana |
|---|---|---|
| **Volume** | Data asli (906 klaim bersih) dibandingkan data skala (replikasi 1.000 kali, 906.000 klaim) untuk mengukur beban komputasi. Salinan tidak menambah informasi, hanya untuk benchmark waktu. | `main.py` tahap 3, tab *Volume & Velocity* |
| **Velocity** | Klaim dipecah menjadi 8 batch kuartalan, diingest berurutan, dan latensi tiap batch diukur. Waktu Operasi A vs B diukur (median 3 kali jalan setelah pemanasan). | `main.py` tahap 1 dan 4-5 |
| **Variety** | CSV klaim (terstruktur) diintegrasikan dengan JSON profil pasien lewat `PATIENT_ID`, keduanya dibaca dengan skema eksplisit. | `main.py` tahap 0 dan 5, tab *Variety & Data Explorer* |
| **Veracity** | Profil kualitas data mentah, pembersihan terdokumentasi, ukuran sebelum dan sesudah. | `main.py` tahap 2, tab *Veracity & Pengujian* |
| **Value** | Lima aturan flag untuk klaim tidak wajar, Top 5 prosedur, Top 10 klaim, temuan penggantian sendi. | `main.py` tahap 5-6, tab *Value* |

### Operasi A vs Operasi B

| | Operasi A: agregasi sederhana | Operasi B: relasional dan temporal |
|---|---|---|
| Pertanyaan | Berapa total pembayaran tiap provider? | Klaim mana yang tidak wajar? |
| Data | Satu tabel (CSV klaim) | Dua sumber (CSV klaim + JSON pasien) |
| Spark | `groupBy` + `agg` | join, statistik per CPT, window per CPT dan per pasien (30 hari), lima flag |
| Peran | Pembanding dasar | Analisis yang dibutuhkan untuk mendeteksi anomali |

Kedua operasi membaca data yang sama, jadi selisih waktunya berasal dari **struktur query** (jumlah join, window,
dan shuffle), bukan jumlah baris. Jumlah `Exchange` (shuffle) pada rencana eksekusi dicatat sebagai buktinya.

### Aturan pembersihan data (Veracity)

1. Klaim ganda: dipertahankan satu baris per `CLAIM_ID`, salinan lain dicatat di berkas karantina.
2. Spesialisasi provider kosong: dipulihkan dari pemetaan `PROVIDER_NPI` ke spesialisasi (hanya bila NPI itu punya tepat satu spesialisasi).
3. Nilai kosong lain yang tidak bisa dipulihkan: dikarantina, tidak ditebak.
4. Tagihan bernilai nol atau negatif: dikarantina (nilai uang tidak diperbaiki dengan menebak).
5. Biaya pasien lebih besar dari bayar asuransi: tidak dihapus, hanya diberi flag.

### Lima aturan flag klaim tidak wajar

| Aturan | Ambang |
|---|---|
| z-score harga dalam satu kode CPT | >= 2,0 |
| Rasio terhadap median CPT | >= 5x |
| Spesialisasi provider jarang untuk CPT tersebut | < 15% klaim CPT |
| Jumlah klaim pasien dalam 30 hari | >= 10 |
| Biaya pasien lebih besar dari bayar asuransi | ya |

---

## 3. Arsitektur

```
data/raw (CSV + JSON)
   -> Ingestion (spark.read dengan skema eksplisit, simulasi batch kuartalan)
   -> Processing / Veracity (profil kualitas, dedup, pulihkan spesialisasi, karantina)
   -> Storage (data/processed: CSV bersih, karantina, JSON pasien, Parquet)
   -> Integration (join CSV x JSON pada PATIENT_ID, tambah RISK_TIER)
   -> Analytics (Operasi A dan Operasi B)
   -> Output (output/*.csv + pipeline_metrics.json -> dashboard Flask)
```

## 4. Struktur Folder

```
Healthcare-Insurance-Claims-Analysis/
├── data/
│   ├── raw/                      <- INPUT (disediakan)
│   │   ├── Dataset_HealthcareInsurance.csv
│   │   └── Dataset_HealthcareInsurance.json
│   └── processed/                <- dibuat otomatis oleh pipeline (data bersih)
│       ├── claims_clean.csv
│       ├── claims_quarantine.csv
│       ├── patients_clean.json
│       └── claims_clean.parquet/
├── output/                       <- dibuat otomatis oleh pipeline (untuk dashboard)
│   ├── flagged_claims.csv
│   ├── summary_cpt_risk.csv
│   ├── provider_totals.csv
│   ├── pipeline_metrics.json
│   ├── pipeline_last_run.log
│   └── incoming_batches/
├── templates/
│   └── index.html                <- frontend dashboard
├── app.py                        <- dashboard Flask
├── main.py                       <- pipeline PySpark
├── requirements.txt
└── README.md
```

---

## 5. Cara Menjalankan

### Prasyarat

| Kebutuhan | Keterangan |
|---|---|
| Python | 3.9 atau lebih baru |
| Java (JDK) | **Wajib** untuk PySpark. JDK 17 direkomendasikan (8 dan 11 juga didukung Spark 3.5). Cek dengan `java -version`. |
| Windows saja | `winutils.exe` dan `hadoop.dll` di `C:\hadoop\bin`. `main.py` sudah mengatur `HADOOP_HOME=C:\hadoop`. Jika foldernya berbeda, ubah dua baris `os.environ` di bagian atas `main.py`. |
| Internet | Dibutuhkan saat membuka dashboard (Tailwind, Chart.js, Mermaid, dan FontAwesome dimuat dari CDN). |

### Langkah 1: Siapkan lingkungan

```bash
# dari folder proyek
python -m venv venv

# aktifkan virtual environment
venv\Scripts\activate          # Windows
source venv/bin/activate       # macOS / Linux

pip install -r requirements.txt
```

### Langkah 2: Pastikan dataset ada

Kedua berkas harus ada di `data/raw/`:

```
data/raw/Dataset_HealthcareInsurance.csv
data/raw/Dataset_HealthcareInsurance.json
```

### Langkah 3: Jalankan (pilih salah satu)

**Opsi A, lewat dashboard web (disarankan untuk demo dan video):**

```bash
python app.py
```

Buka http://127.0.0.1:5000, lalu klik tombol **EXECUTE PYSPARK PIPELINE**. Layar loading muncul selama Spark berjalan
(beberapa menit, tergantung komputer). Setelah selesai, dashboard otomatis menampilkan hasilnya.

**Opsi B, lewat terminal (pipeline saja):**

```bash
python main.py
```

Waktu eksekusi, ukuran kualitas data, dan hasil analitik ditampilkan di konsol (baris berawalan `[TIMER]`
dan tabel benchmark di akhir). Setelah itu jalankan `python app.py` untuk melihat dashboard.

Opsi tambahan untuk `main.py`:

```bash
python main.py --raw-dir data/raw --processed-dir data/processed --out output --scale 1000
```

`--scale` mengatur faktor replikasi data untuk benchmark (bawaan 1000; kecilkan, misalnya 100, bila komputer lambat).

### Langkah 4: Lihat hasil

- Dataset bersih: folder `data/processed/`
- Hasil analitik: folder `output/`
- Dashboard: http://127.0.0.1:5000 (6 tab: Arsitektur, Value, Volume & Velocity, Veracity & Pengujian, Variety & Data Explorer, Naskah Presentasi)

Tab **Veracity & Pengujian** menjalankan pemeriksaan otomatis VER-01 sampai VER-07 atas berkas input, data bersih, dan output.

### Variabel lingkungan (opsional)

| Variabel | Fungsi | Bawaan |
|---|---|---|
| `PIPELINE_TIMEOUT_S` | Batas waktu eksekusi pipeline dari dashboard (detik) | 1800 |
| `FLASK_SECRET_KEY` | Kunci sesi untuk pesan flash | nilai lokal |

---

## 6. Hasil Referensi

Angka berikut dihitung dari dataset pada `data/raw/` dan diverifikasi dengan replika pandas.
**Samakan dengan keluaran konsol dari run Spark milikmu sendiri**, dan gunakan angka dari run-mu untuk laporan dan video.
Waktu eksekusi tidak dicantumkan karena bergantung pada perangkat.

| Ukuran | Nilai |
|---|---|
| Baris mentah, bersih, karantina | 942, 906, 36 (25 klaim ganda + 11 klaim bernilai negatif) |
| Spesialisasi kosong dipulihkan | 21 |
| Baris bermasalah, sebelum dan sesudah | 6,16% menjadi 0,11% (sisa: 1 klaim copay > bayar yang di-flag) |
| Klaim ter-flag | 51 dari 906 (5,6%), bernilai $136.428 atau 26,9% dari total $506.928 |
| Temuan utama | 10 klaim penggantian sendi (CPT 27132 dan 27447) dibayar $9.934 sampai $12.435, sedangkan klaim sendi lainnya $332 sampai $1.152. Tujuh dari sepuluh klaim mahal itu ditagih Internal Medicine atau Nurse Practitioner. |

## 7. Keterbatasan

- Hanya 10 pasien, 7 provider, dan 2 tahun data, sehingga hasil tidak dapat digeneralisasi.
- Tanpa data diagnosis atau lama rawat. Klaim penggantian sendi bernilai tinggi bisa saja sah (rawat inap, implan).
- `RISK_TIER` diturunkan dari total pembayaran sehingga sirkular, tidak untuk membuktikan beban klaim pasien berisiko tinggi.
- Velocity adalah simulasi batch, bukan aliran data nyata. Waktu pada data skala bukan prediksi pasti untuk data nyata sebesar itu.
- **Flag adalah indikasi untuk ditinjau, bukan bukti kecurangan.**

## 8. Pemecahan Masalah

| Gejala | Penyebab dan solusi |
|---|---|
| `JAVA_GATEWAY_EXITED` atau `java not found` | Java belum terpasang atau belum ada di PATH. Pasang JDK 17 lalu buka ulang terminal. |
| Error `winutils` atau `HADOOP_HOME` (Windows) | Pastikan `C:\hadoop\bin\winutils.exe` dan `hadoop.dll` ada, atau sesuaikan path di bagian atas `main.py`. |
| `Python worker failed to connect back` | Jalankan dari dalam virtual environment yang aktif dan pastikan hanya satu versi Python yang dipakai. |
| Dashboard menampilkan "belum ada hasil" | Pipeline belum pernah dijalankan. Klik **EXECUTE PYSPARK PIPELINE** atau jalankan `python main.py`. |
| Chart atau diagram tidak muncul | Butuh internet untuk memuat library dari CDN. Periksa koneksi lalu muat ulang halaman. |
| Chart provider kosong | Pakai `main.py` terbaru lalu jalankan ulang pipeline (menulis `output/provider_totals.csv`). |
| Port 5000 sudah dipakai | Ubah `port=5000` di baris terakhir `app.py`. |
| Pipeline lambat | Kecilkan faktor skala: `python main.py --scale 100`. |
| Pipeline gagal dari dashboard | Buka panel *Pipeline console* di tab Veracity & Pengujian, atau berkas `output/pipeline_last_run.log`. |