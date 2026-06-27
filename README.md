# P2 - Data Processing & Storage

Pipeline ini membaca data tap-in/tap-out Transjakarta dari Kafka topic
`transjakarta-raw`, lalu memprosesnya melalui 3 layer Delta Lake
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
streamlit run dashboard/app.py
```
> **Note:** Jika server FastAPI P3 sedang mati, dashboard otomatis menggunakan data *fallback* sintetis agar UI dan Peta tetap berfungsi dengan baik untuk keperluan demonstrasi.