# P4 · Dashboard & Visualisasi GIS — Suroboyo Bus & Wara-Wiri

Dashboard Streamlit interaktif untuk visualisasi dan prediksi *demand* penumpang Suroboyo Bus dan angkutan *feeder* Wara-Wiri di Surabaya secara *real-time*.

## ✨ Fitur Utama
- 🗺️ **Peta GIS Interaktif (Folium)** — Heatmap halte per rute dengan status kapasitas (SURGE/NORMAL/LOW).
- 📈 **Grafik Prediksi 24 Jam** — Menampilkan tren kepadatan penumpang dan *confidence interval* sepanjang hari.
- 🚦 **Rekomendasi Armada (Machine Learning)** — Rekomendasi dinamis kebutuhan armada untuk mengatasi *surge*.
- ⛅ **Data Cuaca BMKG Real-Time** — *Live-sync* dengan API BMKG untuk fitur prediktif.
- ⏱️ **Estimasi Headway** — Model XGBoost untuk memprediksi jeda kedatangan bus.
- 🗃️ **Mode Fallback** — Dashboard tidak akan *crash* jika layanan *backend* (API P3) mati, melainkan berpindah otomatis ke *Synthetic Mode* untuk keperluan presentasi/demo.

---

## 🚀 Panduan Menjalankan Demo (End-to-End)

Karena komponen **P4 (Dashboard)** bergantung pada **P3 (Machine Learning API)**, kamu harus menjalankan keduanya untuk mendemonstrasikan sistem yang *fully integrated*.

### 1️⃣ Jalankan Backend ML (P3) — *Terminal 1*
Dashboard memerlukan model *Machine Learning* untuk inferensi *real-time*.
Buka terminal baru di *root* proyek:
```bash
cd ml/
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```
> *Tanda berhasil: "Application startup complete"*

### 2️⃣ Jalankan Dashboard Streamlit (P4) — *Terminal 2*
Buka terminal baru lagi dan jalankan Streamlit:
```bash
cd dashboard/
pip install -r requirements.txt
python -m streamlit run app.py
```
> *Dashboard akan terbuka otomatis di browser pada http://localhost:8501*

---

### 🌟 Menjalankan *Full Pipeline* (P1 & P2 Background Services)
Jika kamu ingin mendemonstrasikan keseluruhan ekosistem Big Data *(Ingestion → Processing → Analytics)* yang sedang berjalan *live*:

1. **Nyalakan Kafka & Producers (P1)**
   Buka *Terminal 3*:
   ```bash
   cd kafka/
   docker-compose up -d
   python producer_suroboyo_bus.py  # (Terminal 4)
   python producer_bmkg.py          # (Terminal 5)
   ```
2. **Nyalakan Apache Spark Delta Lake (P2)**
   Buka *Terminal 6*:
   ```bash
   python spark/delta_layers.py
   ```

*(Dashboard ini dirancang asinkron dan tangguh; P1 dan P2 berjalan independen dari P4 di layer arsitektur).*

---

## 🔌 Detail Integrasi API (P3)

Dashboard mengirim `POST` request ke FastAPI setiap kali kamu mengubah filter di *Sidebar* (Rute, Jam, Tanggal, atau Cuaca).

**Contoh Request Body:**
```json
{
  "koridor": "Suroboyo Bus: Purabaya - Rajawali",
  "jam": 8,
  "tanggal": "2026-06-27",
  "suhu": 32.5,
  "hujan": 0,
  "feeder": 0,
  "day_of_week": "Saturday",
  "is_weekend": 1
}
```

**Response dari API:**
```json
{
  "prediksi_penumpang": 45,
  "armada_rekomendasi": 1,
  "status": "NORMAL",
  "confidence": 0.88,
  "demand_level": "sedang",
  "headway_pred": 12.5,
  "headway_status": "BAIK",
  "source": "ml_model"
}
```

## 📁 Struktur File P4
```
dashboard/
├── app.py            ← File utama Streamlit
├── style.css         ← Kustomisasi antarmuka/UI
├── README.md         ← Panduan ini
└── requirements.txt  ← Dependensi Python
```
