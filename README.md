# 🚌 Suroboyo Bus — Big Data Pipeline

Sistem Big Data *end-to-end* untuk pemantauan dan prediksi kepadatan penumpang transportasi publik **Suroboyo Bus** di Kota Surabaya. Sistem ini dirancang untuk menjawab tantangan operasional secara komprehensif, sesuai dengan pilar-pilar analitik Big Data.

## 📋 Pembagian Tugas (P1 - P5)
Proyek ini dibangun melalui 5 tahapan utama pipeline Big Data:
- **P1 (Data Ingestion):** Pengumpulan data secara *real-time* dari API posisi bus (Klacak API) dan API cuaca (BMKG) menggunakan ekosistem **Apache Kafka**.
- **P2 (Data Processing):** Pemrosesan aliran data masif menggunakan **Apache Spark Streaming** dan diorganisasi dengan arsitektur Medallion (*Bronze, Silver, Gold*) di **Delta Lake**.
- **P3 (Machine Learning & API):** Pengembangan model prediksi *demand* penumpang dan *headway* menggunakan **XGBoost**, yang di-serve melalui **FastAPI**.
- **P4 (Data Visualization):** Penyajian data hasil prediksi, rekomendasi armada, dan metrik spasial melalui *dashboard* interaktif **Streamlit**.
- **P5 (Data Engineering & Feature Store):** Integrasi, *polling* periodik 16 rute bus, serta konstruksi data tabular (*Feature Engineered*) siap pakai yang menghubungkan raw data dan model ML.

## 🎯 Overview & Latar Belakang (Identifikasi Masalah)
Masalah utama transportasi publik di Surabaya adalah **ketidakseimbangan alokasi armada** pada jam sibuk (*surge*) dan **kurangnya prediktabilitas waktu kedatangan (headway)** akibat faktor cuaca dan lalu lintas. Data *tracking* GPS puluhan bus yang masuk setiap detik (*Velocity* & *Volume*) digabungkan dengan cuaca *real-time* (*Variety*) tidak bisa lagi diproses dengan database relasional biasa. Kami menggunakan pendekatan Big Data untuk memprediksi lonjakan penumpang sebelum terjadi, mengatasi *gap* dari sistem *monitoring* konvensional yang hanya bersifat reaktif.

### 🌟 Nilai Tambah & Inovasi (Rubrik Penilaian)
1. **Infrastruktur Terdistribusi (End-to-End Pipeline):** Pipeline lengkap mulai dari *Ingestion* (Kafka), *Storage/Processing* (Apache Spark & Delta Lake), hingga *Serving* (FastAPI & Streamlit).
2. **Implementasi Data Lakehouse (Medallion Architecture):** Transformasi terstruktur memisahkan data mentah (*Bronze*), data bersih (*Silver*), hingga data agregasi kaya fitur (*Gold*) yang siap digunakan model ML.
3. **Teknik Analisis Lanjutan (ML & GIS):** Menggabungkan 2 model *Machine Learning* (XGBoost Classifier untuk *Demand Level* & XGBoost Regressor untuk *Estimasi Headway*) dengan Analisis Spasial (Peta Folium GIS). Output terukur melalui probabilitas *confidence interval* dan RMSE model.
4. **Keunikan Solusi:** Menggabungkan 3 domain teknologi secara sinergis: *Real-time Streaming* (Kafka), *Machine Learning* (XGBoost), dan *GIS Spatial Mapping*. Solusi ini unik karena bersifat prediktif alih-alih sekadar pelaporan historis.
5. **Implementasi Andal (Graceful Fallback):** Sistem dibangun secara dinamis. Jika aliran data sensor terputus, *dashboard* tetap beroperasi menggunakan mode *synthetic fallback* berbasis pola *heuristic*.

---

## 🗺️ Arsitektur Sistem

```
[Klacak Bus API]  [BMKG API]  [Dataset Statis]
       │                │              │
       ▼                ▼              │
   Kafka Producer  Kafka Producer     │
  (suroboyo-bus-live) (bmkg-raw)     │
       │                │              │
       └────────┬───────┘              │
                ▼                      │
        Apache Spark (P2)              │
        Delta Lake Medallion           │
        Bronze → Silver → Gold         │
                │                      │
                ▼                      │
       Notebook P5 (Data Engineering)  │
       (Polling Klacak API → CSV)      │
                │                      │
                ▼                      │
        FastAPI ML API (P3)            │
        POST /predict                  │
                │                      │
                └──────────┬───────────┘
                           ▼
                  Streamlit Dashboard (P4)
                  Peta · Grafik · Tabel
```

---

## 📡 API yang Digunakan

### 1. Klacak Bus API — Posisi Bus Real-Time

**Base URL:** `https://busmapapi.fly.dev`

| Endpoint | Method | Keterangan |
|----------|--------|------------|
| `/all` | GET | Bootstrap: ambil `apiUrl` & token autentikasi semua koridor |
| `{apiUrl}/track/{type}/{id}` | GET | Posisi GPS bus real-time per koridor |

**Tipe Bus:**
- `sbybus` — koridor 1, 12, 51 (Suroboyo Bus)
- `temanbus` — koridor 10–99
- `feeder` — koridor ≥ 100 (Angkutan Feeder)

**Contoh response per bus:**
```json
{ "info": "B 1234 SBY", "lat": -7.30, "lng": 112.72, "direction": 180, "speed": 25 }
```

**Penggunaan dalam proyek:**
- `kafka/producer_suroboyo_bus.py` — menarik 4 rute (1, 12, 51, 10) setiap 5 detik, publish ke Kafka topic `suroboyo-bus-live`
- `Notebook_P5.ipynb` — polling **semua 16 rute** setiap 30 detik untuk *Data Engineering* & *Feature Store*

**Daftar 16 Rute:**

| Key | Kode | Nama Rute |
|-----|------|-----------|
| sbr1 | 1 | R1 — Purabaya – Perak |
| tmk2 | 10 | R2 — Kejawan – UNESA |
| sbr4 | 51 | R4 — Purabaya – UNAIR Kampus C |
| sbr5 | 12 | R5 — Term. Benowo – Tunjungan |
| sbrt | 3 | SBT — Bus Tumpuk |
| fd02 | 102 | FD2 — Mayjend Sungkono – Balai Kota |
| fd03 | 108 | FD3 — TIJ – Gunung Anyar |
| fd04 | 121 | FD4 — SIER – Kota Lama |
| fd05 | 105 | FD5 — Mayjend Sungkono – Puspa Raya |
| fd06 | 106 | FD6 — TIJ – Lakarsantri |
| fd07 | 107 | FD7 — Term. Bratang – Stasiun Psr. Turi |
| fd08 | 120 | FD8 — TOW – UNESA |
| fd09 | 122 | FD9 — Term. Menanggal – Term. Manukan |
| fd10 | 123 | FD10 — Term. Keputih – Bunguran |
| fd11 | 124 | FD11 — Term. Bratang – Shelter Bulak |
| fd12 | 127 | FD12 — Purabaya – ITS – Kenjeran Park |

**Data Statis (GitHub):**
```
https://raw.githubusercontent.com/DoubleA4/busmapsby/main/routedata.json  → rute & polyline
https://raw.githubusercontent.com/DoubleA4/busmapsby/main/halte.json      → 927 halte + koordinat
```

---

### 2. BMKG API — Cuaca Real-Time Surabaya

**Base URL:** `https://api.bmkg.go.id`

| Endpoint | Method | Keterangan |
|----------|--------|------------|
| `/publik/prakiraan-cuaca?adm4=35.78.01.1001` | GET | Prakiraan cuaca per wilayah (kode Surabaya) |

**Response yang dipakai:**
```json
{ "t": 32, "weather_desc": "Cerah Berawan", "hu": 75, "ws": 10 }
```

| Field | Keterangan |
|-------|------------|
| `t` | Suhu udara (°C) |
| `weather_desc` | Deskripsi kondisi cuaca |
| `hu` | Kelembapan (%) |
| `ws` | Kecepatan angin (km/h) |

**Penggunaan dalam proyek:**
- `kafka/producer_bmkg.py` — polling setiap **5 menit**, publish ke Kafka topic `bmkg-raw`
- `dashboard/app.py` — dipanggil langsung (tanpa Kafka) setiap **5 menit** untuk menampilkan cuaca Surabaya saat ini di sidebar

---

### 3. FastAPI ML API (Internal — P3)

**Base URL:** `http://localhost:8000`

| Endpoint | Method | Keterangan |
|----------|--------|------------|
| `GET /` | GET | Status service |
| `GET /health` | GET | Status model (loaded/unloaded) |
| `POST /predict` | POST | Prediksi demand & headway |
| `GET /model-info` | GET | Metrik evaluasi & daftar fitur |

**Request `POST /predict`:**
```json
{
  "koridor":     "1",
  "jam":         8,
  "tanggal":     "2026-06-27",
  "suhu":        30.5,
  "hujan":       0,
  "feeder":      0,
  "day_of_week": "Friday",
  "is_weekend":  0
}
```

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
|-------|------------|
| `status` | `SURGE` (>80% kapasitas), `NORMAL` (40–80%), `LOW` (<40%) |
| `demand_level` | `tinggi` / `sedang` / `rendah` — output XGBoost Classifier |
| `headway_pred` | Estimasi waktu tunggu antar bus (menit) — output XGBoost Regressor |
| `headway_status` | `BAIK` jika headway ≤ 15 menit (standar SPM Dishub), `BURUK` jika melebihi |
| `source` | `ml_model` jika model terload, `heuristic_fallback` jika model belum ada |

**Penggunaan dalam proyek:**
- `dashboard/app.py` — dipanggil setiap kali pengguna mengganti filter (koridor, jam, tanggal, cuaca)

---

## 🏗️ Implementasi 5V Big Data

| V | Implementasi dalam Proyek |
|---|--------------------------|
| **Volume** | Kafka menyimpan ratusan ribu event bus per hari. Delta Lake mempartisi data per `ingest_date` dan `koridor` agar query historis tetap efisien dalam skala masif. |
| **Velocity** | Producer Kafka polling API Klacak setiap **5 detik**. Spark Structured Streaming memproses data dalam *micro-batch*. Feature Store diperbarui setiap **1 menit**. |
| **Variety** | Sistem mengintegrasikan data GPS (JSON real-time), data cuaca (JSON dari BMKG), data halte (CSV/Excel geospasial), dan data armada (Excel statis). |
| **Veracity** | Layer **Silver** Spark membersihkan data: parsing JSON, deduplikasi transaksi, filter anomali (tap-in tanpa tap-out), dan normalisasi missing values. |
| **Value** | Dashboard memberikan rekomendasi alokasi armada berbasis ML (XGBoost) yang dapat langsung ditindaklanjuti operator, dengan visualisasi kepadatan halte secara real-time. |

---

## 📁 Struktur Direktori

```
fp-bigdata/
├── kafka/                     # P1 — Data Ingestion
│   ├── config.py              # Konfigurasi Kafka server & topic
│   ├── create_topics.py       # Inisialisasi Kafka topics
│   ├── producer_suroboyo_bus.py  # Producer: live bus tracking (Klacak API)
│   ├── producer_bmkg.py       # Producer: cuaca (BMKG API)
│   ├── producer_events.py     # Producer: data event/hari libur
│   ├── consumer_all.py        # Consumer: baca semua topic
│   ├── docker-compose.yml     # Kafka & Zookeeper via Docker
│   └── requirements.txt
│
├── spark/                     # P2 — Data Processing
│   └── delta_layers.py        # Spark Streaming: Bronze→Silver→Gold
│
├── ml/                        # P3 — Machine Learning & API
│   ├── train_xgboost.py       # Training XGBoost Classifier
│   ├── train_regressor.py     # Training XGBoost Regressor
│   ├── api/
│   │   └── main.py            # FastAPI: POST /predict
│   ├── models/                # File .pkl model terlatih
│   └── requirements.txt
│
├── dashboard/                 # P4 — Visualisasi
│   ├── app.py                 # Streamlit dashboard utama
│   ├── style.css              # Custom CSS
│   └── requirements.txt
│
├── dataset/                   # Data statis referensi
│   ├── Halte_Suroboyo_dengan_Koordinat.csv
│   ├── Data Koridor SuroboyoBus & WaraWiri API.xlsx
│   └── Data Armada SuroboyoBus 2025.xlsx
│
├── Notebook_P5.ipynb          # P5 — Data Engineering (polling + feature store)
├── feature_engineered.csv     # Output P5 — input training model P3
└── verify_delta.py            # Utilitas cek Delta Lake
```

---

## 🚀 Cara Menjalankan

### Prasyarat
- Python 3.11+
- Java 21 (untuk Spark)
- Docker (untuk Kafka)

### 1. Jalankan Kafka (P1)

```bash
cd kafka/
docker-compose up -d          # Start Kafka & Zookeeper
python create_topics.py       # Buat topics
python producer_suroboyo_bus.py  # Start producer bus
python producer_bmkg.py          # Start producer cuaca
```

### 2. Jalankan Spark (P2)

```bash
pip install pyspark==4.1.1 delta-spark==4.3.0
python spark/delta_layers.py
```

### 3. Jalankan Data Engineering (P5)

Buka dan jalankan semua cell di `Notebook_P5.ipynb`. Ini akan menghasilkan `feature_engineered.csv`.

### 4. Training Model (P3)

```bash
cd ml/
pip install -r requirements.txt
python train_xgboost.py    # Classifier
python train_regressor.py  # Regressor
```

### 5. Jalankan API (P3)

```bash
cd ml/
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
# Docs: http://localhost:8000/docs
```

### 6. Jalankan Dashboard (P4)

```bash
cd dashboard/
pip install -r requirements.txt
python -m streamlit run app.py
```

> **Tip:** Jika FastAPI tidak berjalan, dashboard otomatis menggunakan data *synthetic fallback* agar tampilan tetap bisa didemonstrasikan.

---

## 📊 Dashboard (P4) — Detail Implementasi

Dashboard berbasis **Streamlit** ini adalah antarmuka utama yang dikonsumsi operator/pengguna akhir.

### Sumber Data yang Dikonsumsi

| Sumber | Tipe | Keterangan |
|--------|------|------------|
| `dataset/Halte_Suroboyo_dengan_Koordinat.csv` | File lokal | Koordinat GPS setiap halte |
| `dataset/Data Koridor SuroboyoBus & WaraWiri API.xlsx` | File lokal | Daftar koridor & metadata rute |
| `dataset/Data Armada SuroboyoBus 2025.xlsx` | File lokal | Jumlah armada Suroboyo Bus (diesel + listrik) |
| `https://api.bmkg.go.id/...` | REST API | Cuaca real-time Surabaya (cache 5 menit) |
| `http://localhost:8000/predict` | REST API (P3) | Prediksi demand & headway dari ML model |

### Fitur yang Ditampilkan

#### 📐 Sidebar (Panel Filter)
- Pilih **Koridor** (dropdown dinamis dari dataset)
- Pilih **Jam Prediksi** (slider 05:00–22:00)
- Pilih **Tanggal** (otomatis deteksi hari kerja vs akhir pekan)
- **Cuaca:** Toggle antara API BMKG real-time atau simulasi manual (suhu + toggle hujan)
- **Total Armada Suroboyo Bus:** jumlah unit fisik (diesel + listrik) dari dataset armada
- Toggle **Auto-refresh** setiap 30 detik

#### 🔢 Metric Cards (6 Kartu — 2 Baris)

| Baris | Kartu | Nilai | Sumber |
|-------|-------|-------|--------|
| 1 | Prediksi Penumpang | Jumlah orang per bus per jam | `POST /predict` |
| 1 | Armada Rekomendasi | Jumlah bus yang harus dikerahkan | `POST /predict` |
| 1 | Tingkat Pengisian | % kapasitas bus terisi | Dihitung dari prediksi |
| 2 | Status Koridor | SURGE / NORMAL / LOW | `POST /predict` |
| 2 | Tingkat Permintaan (ML) | TINGGI / SEDANG / RENDAH | `demand_level` dari P3 |
| 2 | Estimasi Headway (ML) | Menit + status BAIK/BURUK | `headway_pred` dari P3 |

#### 🗺️ Peta Interaktif (Folium)
- Menampilkan semua halte pada koridor yang dipilih
- Warna marker:
  - 🔴 **Merah** = SURGE (≥80% kapasitas)
  - 🟢 **Hijau** = NORMAL (40–80%)
  - 🔵 **Biru** = LOW (≤40%)
- Klik marker → popup nama halte, estimasi penumpang, dan koordinat

#### 📈 Grafik Prediksi 24 Jam
- Line chart prediksi penumpang jam 05:00–22:00
- Menampilkan confidence interval (±12%)
- Garis threshold SURGE (80% kapasitas)
- Marker vertikal pada jam yang dipilih

#### 📋 Tabel Rekomendasi Armada (Semua Koridor)
- Menampilkan top-10 koridor berdasarkan jumlah halte
- Kolom: Koridor · Jam · Prediksi Penumpang · Armada · Pengisian (%) · Status (badge warna)
- Diurutkan: SURGE → NORMAL → LOW

#### 🌡️ Heatmap Demand (Koridor × Jam)
- Visualisasi intensitas permintaan untuk semua koridor dalam satu hari (05:00–22:00)
- Warna: biru gelap (sepi) → orange → merah (padat)

### Cara API Dipanggil di Dashboard

```python
# call_fastapi() — dipanggil tiap kali filter berubah
payload = {
    "koridor":     selected_corridor,
    "jam":         selected_jam,
    "tanggal":     selected_date.strftime("%Y-%m-%d"),
    "suhu":        suhu_sim,        # dari BMKG atau slider
    "hujan":       int(hujan_sim),  # 0 atau 1
    "feeder":      selected_feeder, # 0=SB, 1=Feeder
    "day_of_week": day_of_week,     # "Monday", "Saturday", dst.
    "is_weekend":  is_weekend,      # 0 atau 1 (auto-detect dari tanggal)
}
response = requests.post("http://localhost:8000/predict", json=payload)
```

> **Error Handling & Fallback:** Jika `POST /predict` ke FastAPI (P3) gagal, terputus, atau *timeout* (misal: server ML mati/dihentikan), dashboard memiliki sistem *error handling* yang akan memunculkan banner peringatan warna merah (🚨 **API OFFLINE / TERPUTUS!**) langsung di antarmuka pengguna (UI). Sistem kemudian otomatis beralih menggunakan data prediksi simulasi secara deterministik (*synthetic fallback*) agar aplikasi tetap bisa beroperasi penuh dan didemonstrasikan tanpa mengalami *crash*.

---

## 🔧 Kafka Topics

| Topic | Producer | Interval | Keterangan |
|-------|----------|----------|------------|
| `suroboyo-bus-live` | `producer_suroboyo_bus.py` | 5 detik | Posisi GPS bus Suroboyo Bus |
| `bmkg-raw` | `producer_bmkg.py` | 5 menit | Data cuaca BMKG Surabaya |
| `events-raw` | `producer_events.py` | 5 detik | Data event/hari libur |

---

## 📦 Delta Lake — Struktur Output (P2)

```
delta/
├── bronze/            ← Raw data dari Kafka, partisi per ingest_date
├── silver/            ← Data bersih: dedup, filter anomali, parse JSON
└── gold/              ← Agregasi penumpang per halte/jam/koridor
    └── features_csv_tmp/   ← Feature Store CSV, update tiap 1 menit
```

### Skema Feature Store (Gold Layer)

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `koridor` | string | ID koridor bus |
| `halte` | string | Nama halte |
| `tanggal` | date | Tanggal window agregasi |
| `jam` | int | Jam (0–23) |
| `penumpang` | long | Jumlah tap-in pada window tersebut |
| `suhu` | string | Dari topic `bmkg-raw` (nullable) |
| `hujan` | string | Dari topic `bmkg-raw` (nullable) |
| `is_libur` | string | Dari referensi hari libur (nullable) |
| `is_weekend` | boolean | True jika Sabtu/Minggu |

---

## 🤖 Model Machine Learning (P3)

Model ditraining dari `feature_engineered.csv` yang dihasilkan `Notebook_P5.ipynb`.

### XGBoost Classifier — `xgboost_model.pkl`
- **Target:** `demand_level` → `tinggi` / `sedang` / `rendah`
- **Hyperparameter tuning:** Optuna (30 trials, 3-fold Stratified CV)

### XGBoost Regressor
- `xgb_regressor_headway.pkl` → prediksi `headway_real_min`
- `xgb_regressor_nefektif.pkl` → prediksi `n_efektif` (jumlah bus aktif)

### Fitur Input Model

| Fitur | Keterangan |
|-------|------------|
| `hour` | Jam (0–23) |
| `is_peak_enc` | 1 jika jam 06–09 atau 16–19 |
| `is_weekend_enc` | 1 jika Sabtu/Minggu |
| `feeder_enc` | 0 = SuroboyoBus, 1 = Feeder |
| `n_total` | Total bus di koridor |
| `n_efektif` | Jumlah bus aktif beroperasi |
| `pct_efektif` | % bus aktif vs total |
| `headway_real_min` | Headway aktual antar bus (menit) |
| `headway_gap_vs_spm` | Selisih headway vs SPM 15 menit |
| `avg_speed_kmh` | Kecepatan rata-rata bus (km/h) |
| `pct_mangkal` | % bus yang diam/mangkal |
