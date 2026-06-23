# P4 · Dashboard & Visualisasi GIS — Suroboyo Bus

Dashboard Streamlit untuk prediksi demand penumpang Suroboyo Bus secara real-time.

## Fitur
- 🗺️ **Peta GIS Folium** — heatmap halte per koridor (merah=SURGE, hijau=NORMAL, biru=LOW)
- 📈 **Grafik prediksi 24 jam** — dengan confidence interval dan threshold SURGE
- 🚌 **Tabel rekomendasi armada** — semua koridor, diurutkan SURGE dulu
- 🔥 **Heatmap koridor × jam** — pola demand sepanjang hari
- ⚠️ **Alert SURGE otomatis** — banner merah + log ke `alert_log.csv`
- ⏱ **Auto-refresh 30 detik** — bisa diaktifkan di sidebar

## Cara Jalankan

```bash
# Install dependencies
pip install -r requirements.txt

# Jalankan dashboard
streamlit run app.py
```

Dashboard berjalan di `http://localhost:8501`

## Integrasi FastAPI (P3)

Dashboard akan otomatis memanggil `POST http://localhost:8000/predict`.  
Kalau P3 belum siap, dashboard pakai **synthetic fallback** otomatis — tetap bisa demo.

### Format request ke FastAPI:
```json
{
  "koridor": "Koridor 1: Purabaya - Rajawali",
  "jam": 8,
  "tanggal": "2025-04-15",
  "suhu": 32.0,
  "hujan": 0
}
```

### Format response yang diharapkan:
```json
{
  "prediksi_penumpang": 47,
  "armada_rekomendasi": 1,
  "status": "NORMAL",
  "confidence": 0.88
}
```

## Struktur File

```
dashboard/
├── app.py            ← Streamlit app utama
├── requirements.txt  ← Dependencies
├── README.md         ← Ini
└── alert_log.csv     ← Auto-generated saat ada SURGE
```

## Koridor Suroboyo Bus (Built-in)

| Kode | Rute | Kapasitas |
|------|------|-----------|
| K1   | Purabaya → Rajawali | 60 org |
| K2   | Rajawali → Lidah Wetan | 60 org |
| K3   | Puspa Agro → Kenjeran | 55 org |

> Koordinat halte bisa diupdate dari data.surabaya.go.id kalau tersedia GeoJSON resmi.
