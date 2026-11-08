import os
import pandas as pd

# 1. Tentukan jalur file Excel
excel_path = "data/ABC Healthcare Company Claims Dataset.xlsx"

# Periksa apakah file Excel ada
if not os.path.exists(excel_path):
    print(f"Error: File '{excel_path}' tidak ditemukan. Pastikan file ada di folder 'data/'.")
    exit()

# 2. Baca file Excel
print("Membaca file Excel...")
df = pd.read_excel(excel_path)

# Tampilkan kolom yang ada di file Excel untuk verifikasi
print("\nKolom yang terdeteksi di Excel:")
print(df.columns.tolist())

# ==========================================
# 3. Ekspor ke CSV (Data Transaksi Klaim)
# ==========================================
# Ambil seluruh dataset transaksi dan simpan sebagai CSV
csv_output_path = "data/claims_transactions.csv"
df.to_csv(csv_output_path, index=False)
print(f"\n✓ Berhasil mengekspor: {csv_output_path}")

# ==========================================
# 4. Ekspor ke JSON (Data Profil Pasien)
# ==========================================
# Pilih kolom yang berkaitan dengan atribut pasien/penyakit
# (Sesuaikan nama kolom di bawah dengan nama kolom persis dari print di atas)
patient_columns = [col for col in df.columns if any(k in col.lower() for k in ['patient', 'age', 'gender', 'risk', 'diagnosis'])]

# Jika kolom spesifik pasien ditemukan, filter dan hapus duplikat berdasarkan Patient ID
if patient_columns:
    patients_df = df[patient_columns].drop_duplicates()
else:
    # Jika tidak ada pemisahan kolom khusus, ambil dataset utama
    patients_df = df.drop_duplicates()

json_output_path = "data/patients_info.json"
patients_df.to_json(json_output_path, orient="records", indent=4)
print(f"✓ Berhasil mengekspor: {json_output_path}")

print("\nKonversi selesai! Folder 'data/' sekarang memiliki file CSV dan JSON untuk PySpark.")