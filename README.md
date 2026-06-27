# P2 - Data Processing & Storage

Pipeline ini membaca data tap-in/tap-out Transjakarta dari Kafka topic
`suroboyo-bus-live`, lalu memprosesnya melalui 3 layer Delta Lake
(Bronze -> Silver -> Gold) dan mengekspor Feature Store CSV untuk dipakai
oleh P3 (Machine Learning) dan P5 (Data Engineering).

## Struktur Output

```
delta/
├── bronze/   raw data apa adanya, partisi per tanggal (ingest_date)
├── silver/   data bersih: dedup, filter anomali tap-in tanpa tap-out
└── gold/     agregasi penumpang per halte/jam/koridor, partisi per koridor+tanggal
    └── features_csv_tmp/   Feature Store CSV, update tiap 1 menit
```

## Skema Feature Store (Gold)

| Kolom        | Tipe      | Keterangan                                      |
|--------------|-----------|--------------------------------------------------|
| koridor      | string    | ID koridor Transjakarta                          |
| halte        | string    | Nama halte                                       |
| tanggal      | date      | Tanggal window agregasi                          |
| jam          | int       | Jam window agregasi (0-23)                       |
| penumpang    | long      | Jumlah event tap-in pada window tersebut         |
| suhu         | string    | NULL - belum disambungkan ke topic BMKG (P1)     |
| hujan        | string    | NULL - belum disambungkan ke topic BMKG (P1)     |
| is_libur     | string    | NULL - belum disambungkan ke referensi hari libur (P1) |
| is_weekend   | boolean   | True jika tanggal jatuh pada Sabtu/Minggu         |

> Catatan untuk P3 & P5: kolom suhu, hujan, is_libur masih placeholder
> NULL. Begitu topic bmkg-raw dan referensi hari libur dari P1 sudah
> tersedia dan terverifikasi formatnya, join akan ditambahkan ke Gold
> layer. Skema kolom tidak akan berubah, jadi aman untuk mulai develop
> dengan asumsi format ini.

## Prasyarat untuk Menjalankan

1. Java 21 (Temurin direkomendasikan - Java 11 terlalu lama untuk
   Spark 4.x, Java 23+ punya bug kompatibilitas dengan Hadoop)
2. Python 3.12 dengan pyspark==4.1.1 dan delta-spark==4.3.0

---
# P3 - Machine Learning & Inference API

Modul ini membangun model prediksi demand penumpang SuroboyoBus dan menyajikannya sebagai REST API yang dikonsumsi oleh dashboard P4.

## Struktur Direktori

```
ml/
├── train_xgboost.py              ← Training classifier demand_level (tinggi/sedang/rendah)
├── train_regressor.py            ← Training regressor headway & jumlah bus efektif
├── requirements.txt              ← Dependensi Python P3
├── api/
│   ├── __init__.py
│   └── main.py                   ← FastAPI endpoint POST /predict
└── models/                       ← Dihasilkan otomatis setelah training
    ├── xgboost_model.pkl
    ├── xgb_regressor_headway.pkl
    ├── xgb_regressor_nefektif.pkl
    ├── label_encoder.pkl
    ├── label_encoders_regressor.pkl
    ├── feature_columns.json
    ├── regressor_feature_columns.json
    └── xgboost_metrics.json
```

## Alur Kerja P3

```
Notebook_P5.ipynb
       │
       ▼ feature_engineered.csv
  train_xgboost.py ──► models/xgboost_model.pkl
  train_regressor.py ─► models/xgb_regressor_*.pkl
       │
       ▼
  api/main.py (FastAPI)
       │
       ▼ POST /predict
  dashboard/app.py (P4)
```

Input utama P3 adalah `feature_engineered.csv` yang dihasilkan langsung oleh **Notebook_P5.ipynb** (polling API Klacak), **bukan** dari pipeline Kafka→Spark P1/P2. Hal ini membuat P3 dapat berjalan secara independen.

## Prasyarat

- Python 3.10+
- `feature_engineered.csv` sudah tersedia (jalankan `Notebook_P5.ipynb` terlebih dahulu)

Install dependensi:

```bash
cd ml/
pip install -r requirements.txt
```

## Menjalankan Training

```bash
cd ml/

# Step 1: Training XGBoost classifier (prediksi demand_level)
python train_xgboost.py

# Step 2: Training XGBoost regressor (prediksi headway & n_efektif)
python train_regressor.py
```

> **Catatan path:** Sesuaikan variabel `FEATURE_FILE` di kedua script jika lokasi `feature_engineered.csv` berbeda dari default (`../feature_engineered.csv`).

Setelah training selesai, seluruh file model tersimpan otomatis di `ml/models/`.

## Menjalankan API

```bash
cd ml/
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

Dokumentasi interaktif tersedia di: **http://localhost:8000/docs**

## Endpoint API

### `GET /`
Cek status service dan daftar endpoint.

### `GET /health`
Cek apakah model sudah terload.

```json
{
  "status": "ok",
  "classifier_loaded": true,
  "hw_regressor_loaded": true,
  "ne_regressor_loaded": true
}
```

### `POST /predict`

**Request:**
```json
{
  "koridor":     "1",
  "jam":         8,
  "tanggal":     "2026-06-27",
  "suhu":        30.5,
  "hujan":       0,
  "n_total":     12,
  "feeder":      0,
  "day_of_week": "Friday",
  "is_weekend":  0
}
```

| Field | Tipe | Keterangan |
|-------|------|-----------|
| `koridor` | string | Kode/nama koridor |
| `jam` | int (0–23) | Jam prediksi |
| `feeder` | int | `0` = trunk SuroboyoBus (kapasitas 60), `1` = feeder Wara-Wiri (kapasitas 15) |
| `suhu` | float | Suhu udara °C (opsional) |
| `hujan` | int | `0` = tidak hujan, `1` = hujan |

**Response:**
```json
{
  "prediksi_penumpang":  36,
  "armada_rekomendasi":  1,
  "status":              "NORMAL",
  "confidence":          0.965,
  "demand_level":        "sedang",
  "headway_pred":        12.0,
  "headway_status":      "BAIK",
  "source":              "ml_model"
}
```

| Field | Keterangan |
|-------|-----------|
| `status` | `SURGE` (>80% kapasitas), `NORMAL` (40–80%), `LOW` (<40%) |
| `headway_status` | `BAIK` jika headway ≤ 15 menit (standar SPM), `BURUK` jika melebihi |
| `source` | `ml_model` jika model terload, `heuristic_fallback` jika model belum ada |

### `GET /model-info`
Metrik evaluasi model dan daftar fitur yang digunakan.

## Model & Fitur

### XGBoost Classifier (demand_level)

Target: `demand_level` — `tinggi` / `sedang` / `rendah`

Fitur yang digunakan:

| Fitur | Keterangan |
|-------|-----------|
| `hour` | Jam (0–23) |
| `is_peak_enc` | 1 jika jam 06–09 atau 16–19 |
| `is_weekend_enc` | 1 jika Sabtu/Minggu |
| `feeder_enc` | 0 = SuroboyoBus, 1 = Wara-Wiri |
| `n_total` | Total bus di koridor |
| `n_efektif` | Jumlah bus yang aktif beroperasi |
| `pct_efektif` | Persentase bus aktif vs total |
| `headway_real_min` | Headway aktual antar bus (menit) |
| `headway_gap_vs_spm` | Selisih headway vs standar SPM 15 menit |
| `avg_speed_kmh` | Kecepatan rata-rata bus |
| `pct_mangkal` | Persentase bus yang mangkal/idle |

Hyperparameter tuning menggunakan **Optuna** dengan 30 trials dan 3-fold Stratified Cross Validation.

### XGBoost Regressor

Dua model regressor ditraining terpisah:
- `xgb_regressor_headway.pkl` → prediksi `headway_real_min`
- `xgb_regressor_nefektif.pkl` → prediksi `n_efektif`

Output regressor digunakan API untuk menghitung rekomendasi armada yang lebih akurat.

## Fallback Mode

Jika file `.pkl` belum ada (model belum ditraining), API tetap berjalan menggunakan **heuristic fallback** berbasis jam peak dan tipe koridor. Field `source` pada response akan bernilai `"heuristic_fallback"`.

Ini memungkinkan dashboard P4 tetap dapat berjalan untuk keperluan demo meski proses training belum selesai.

--- 

# P4 - Dashboard & Visualisasi

Direktori `dashboard/` berisi aplikasi Frontend (berbasis **Streamlit**) yang berfungsi sebagai titik akhir dari keseluruhan *pipeline* Big Data.

## Alur Data (Flow) P4
1. **Membaca Dataset Geospasial**: Dashboard membaca `dataset/Halte_Suroboyo_dengan_Koordinat.csv` untuk memetakan koordinat riil (Latitude/Longitude) dari setiap halte Suroboyo Bus & Wara-Wiri.
2. **Koneksi ke Backend (P3)**: Saat pengguna mengganti filter (Koridor, Waktu, Suhu, Cuaca) di panel samping, dashboard akan mengirim HTTP POST *Request* ke endpoint **FastAPI P3** (`http://localhost:8000/predict`).
3. **Visualisasi Prediksi & Rekomendasi**:
   - Jika API aktif, dashboard menerima angka prediksi penumpang.
   - Peta interaktif (Folium) akan menyesuaikan warna halte (Merah = SURGE, Hijau = NORMAL, Biru = LOW) berdasarkan prediksi penumpang terhadap rasio kapasitas armada.
   - Tabel Rekomendasi Armada menghitung dan menampilkan berapa bus yang perlu di-deploy untuk memenuhi *demand* tersebut tanpa melebihi total unit bus fisik (bersumber dari rekomendasi backend P3).

## Menjalankan Dashboard
Masuk ke root direktori proyek, lalu jalankan:
```bash
 python -m streamlit run app.py
```
> **Note:** Jika server FastAPI P3 sedang mati, dashboard otomatis menggunakan data *fallback* sintetis agar UI dan Peta tetap berfungsi dengan baik untuk keperluan demonstrasi.
