# P4 · Dashboard & Visualisasi GIS — Suroboyo Bus

Dashboard Streamlit interaktif untuk visualisasi dan prediksi *demand* penumpang Suroboyo Bus di Surabaya secara *real-time*.

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

## 🎭 Skenario Demo Presentasi (Wow Factor)

Saat presentasi di depan dosen/reviewer, ikuti langkah-langkah skenario ini agar semua fitur *advanced* terlihat jelas:

1. **Pamerkan Auto-Inference ML (Sangat Penting):**
   - **Tindakan:** Ubah jam prediksi dari siang hari (misal jam `12:00`) menjadi jam sibuk/pulang kerja (jam `17:00` atau `18:00`).
   - **Penjelasan:** Tunjukkan kepada penguji bahwa sistem sangat cerdas. Di *backend*, API (FastAPI) secara otomatis menyadari bahwa jam 17:00 adalah *Rush Hour* (`is_peak=True`). Hal ini membuat prediksi ML langsung loncat (muncul notifikasi **SURGE ALERT** berwarna merah), status koridor berubah menjadi **SURGE**, dan rekomendasi armada otomatis bertambah.

2. **Perbedaan Koridor Suroboyo Bus vs Feeder Wara-Wiri:**
   - **Tindakan:** Ubah pilihan *dropdown* koridor dari Rute Utama (kode awalan SB) ke Rute Feeder (kode awalan FD/Wara-Wiri).
   - **Penjelasan:** Perlihatkan bahwa kapasitas bus langsung berubah dari 60 penumpang (Trunk) menjadi 15 penumpang (Feeder). Tingkat pengisian (%) dan ambang batas *surge* akan menyesuaikan secara otomatis secara dinamis berkat fitur `feeder_enc` di ML.

3. **Interaksi GIS Peta (Folium):**
   - **Tindakan:** Klik pada lingkaran merah/hijau/biru yang ada di dalam peta halte Suroboyo.
   - **Penjelasan:** Sistem GIS terintegrasi, menampilkan nama halte, jumlah penumpang spesifik di halte tersebut, koordinat, dan status kepadatan.

4. **Pamerkan Sistem *Graceful Fallback* (Uji Ketahanan Sistem):**
   - **Tindakan:** Buka Terminal tempat Uvicorn/FastAPI (P3) berjalan, lalu tekan `Ctrl+C` untuk mematikan server ML di tengah-tengah presentasi. Kemudian di dashboard, ubah jam atau ganti koridor.
   - **Penjelasan:** UI **tidak akan crash**. Sebaliknya, akan muncul *banner* peringatan merah besar (🚨 **API OFFLINE / TERPUTUS!**) yang memberi tahu *user* bahwa sistem berpindah secara instan ke mode simulasi data (*synthetic fallback*). Ini akan menunjukkan kedewasaan aplikasi skala *Enterprise* yang kebal terhadap *downtime*.

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
