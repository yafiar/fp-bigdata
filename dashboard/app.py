# =============================================================================
# SUROBOYO BUS — SMART DEMAND DASHBOARD
# Dashboard prediksi kepadatan penumpang bus berbasis Machine Learning
# Final Project Big Data — Streamlit Frontend
# =============================================================================

# ── Import library yang dibutuhkan ─────────────────────────────────────────────
import streamlit as st          # Framework utama untuk membuat web dashboard
import pandas as pd             # Untuk manipulasi dan analisis data tabel
import numpy as np              # Untuk operasi matematika dan angka acak
import plotly.graph_objects as go  # Untuk membuat grafik interaktif (line chart, heatmap)
import plotly.express as px     # Alternatif plotly yang lebih sederhana (tersedia jika diperlukan)
import folium                   # Untuk membuat peta interaktif berbasis Leaflet.js
from streamlit_folium import st_folium  # Komponen untuk menampilkan peta Folium di dalam Streamlit
import requests                 # Untuk mengirim HTTP request ke backend FastAPI
from datetime import datetime, timedelta  # Untuk manipulasi waktu dan tanggal
import time                     # Untuk perintah sleep (jeda waktu) pada auto-refresh
import random                   # Untuk membuat angka acak (dipakai di data sintetis)
import os                       # Untuk mengecek keberadaan file di sistem
import csv                      # Untuk menulis log alert ke file CSV

# =============================================================================
# KONFIGURASI HALAMAN
# st.set_page_config HARUS dipanggil paling pertama sebelum perintah Streamlit lain
# =============================================================================
st.set_page_config(
    page_title="Suroboyo Bus — Smart Demand Dashboard",  # Judul yang muncul di tab browser
    page_icon="bus",       # Ikon di tab browser
    layout="wide",         # Layout melebar memenuhi layar (bukan layout sempit default)
    initial_sidebar_state="expanded",  # Sidebar langsung terbuka saat halaman dimuat
)

# =============================================================================
# MUAT CSS DARI FILE EKSTERNAL (style.css)
# CSS dipisahkan ke file sendiri agar kode Python lebih bersih dan mudah diubah.
# Fungsi open() membaca isi file style.css, lalu disuntikkan ke halaman
# menggunakan st.markdown() dengan unsafe_allow_html=True
# =============================================================================
with open("style.css", "r", encoding="utf-8") as f:
    css_isi = f.read()  # Baca seluruh isi file CSS sebagai string

# Bungkus dengan tag <style> lalu tampilkan ke halaman
st.markdown(f"<style>{css_isi}</style>", unsafe_allow_html=True)



# =============================================================================

# =============================================================================
# DATA DINAMIS — KORIDOR DAN HALTE BUS
# Membaca data koridor dan halte dari dataset (hasil geocoding)
# =============================================================================
import re  # Untuk word-boundary matching pada kode rute

@st.cache_data(ttl=60)
def load_corridors_data():
    file_path = "../dataset/Halte_Suroboyo_dengan_Koordinat.csv"
    koridor_path = "../dataset/Data Koridor SuroboyoBus & WaraWiri API.xlsx"
    
    # Jika dataset belum ada (script geocoding belum selesai/dijalankan)
    if not os.path.exists(file_path):
        st.error("⚠️ Data halte berkoordinat belum tersedia! Jalankan `data_prep/geocode_halte.py` terlebih dahulu.")
        return {}, 0, 0
        
    try:
        df_halte = pd.read_csv(file_path)
        df_koridor = pd.read_excel(koridor_path)
    except Exception as e:
        st.error(f"Gagal membaca dataset: {e}")
        return {}, 0, 0

    total_halte = len(df_halte)
    halte_with_coords = df_halte.dropna(subset=['Latitude', 'Longitude'])
    total_geocoded = len(halte_with_coords)

    corridors_dict = {}
    
    # Kelompokkan halte per koridor menggunakan regex word-boundary
    # agar key "fd05" tidak salah match ke "fd0" atau sebaliknya
    for _, row in df_koridor.iterrows():
        key = str(row['KEY']).strip()
        tipe = str(row['KETERANGAN']).strip()
        nama_rute = f"{tipe} {key.upper()}: {str(row['RUTE']).strip()}"
        
        # Buat regex pattern dengan word boundary
        pattern = re.compile(r'(?:^|[\s/])' + re.escape(key) + r'(?:[\s/]|$)', re.IGNORECASE)
        
        halte_list = []
        for _, h_row in halte_with_coords.iterrows():
            rutes = str(h_row['Rute_yang_Melewati'])
            if pattern.search(rutes):
                lat = h_row['Latitude']
                lon = h_row['Longitude']
                halte_list.append((str(h_row['Nama_Halte']).strip(), float(lat), float(lon)))
        
        # Hanya tambahkan koridor jika minimal ada 1 halte valid
        if halte_list:
            corridors_dict[nama_rute] = {
                "stops": halte_list,
                "capacity": 60 if tipe == "SB" else 15  # Wara-Wiri kapasitas lebih kecil
            }
            
    return corridors_dict, total_halte, total_geocoded

CORRIDORS, TOTAL_HALTE, TOTAL_GEOCODED = load_corridors_data()

# Hentikan eksekusi dashboard jika data koridor kosong (mencegah error lanjutan)
if not CORRIDORS:
    st.stop()

# Daftar jam operasional bus (05:00 sampai 22:00)
HOURS = list(range(5, 23))   # Menghasilkan [5, 6, 7, ..., 22]

# =============================================================================
# FUNGSI PEMBANTU (HELPER FUNCTIONS)
# =============================================================================

def get_status(penumpang: int, capacity: int) -> str:
    """
    Menghitung status kepadatan berdasarkan rasio penumpang terhadap kapasitas.
    - SURGE  : >= 80% penuh (perlu tambah armada segera)
    - LOW    : <= 40% penuh (armada bisa dikurangi)
    - NORMAL : di antara keduanya
    """
    ratio = penumpang / capacity
    if ratio >= 0.80:
        return "SURGE"
    elif ratio <= 0.40:
        return "LOW"
    return "NORMAL"


def status_badge(status: str) -> str:
    """
    Menghasilkan HTML badge berwarna berdasarkan nilai status.
    Warna merah untuk SURGE, hijau untuk NORMAL, biru untuk LOW.
    Digunakan di dalam tabel rekomendasi armada.
    """
    cls = {"SURGE": "badge-surge", "LOW": "badge-low", "NORMAL": "badge-normal"}[status]
    return f'<span class="{cls}">{status}</span>'


@st.cache_data(ttl=30, show_spinner=False)
def is_api_alive() -> bool:
    """
    Mengecek apakah server FastAPI (backend ML) sedang berjalan.
    Menggunakan timeout sangat singkat (0.2 detik) agar tidak memblokir dashboard.
    Hasilnya di-cache selama 30 detik — artinya pengecekan hanya dilakukan sekali
    per 30 detik, bukan setiap kali fungsi dipanggil.
    """
    try:
        requests.get("http://localhost:8000/", timeout=0.2)
        return True
    except Exception:
        # Jika koneksi gagal (timeout, connection refused, dll) → API mati
        return False

@st.cache_data(ttl=60, show_spinner=False)
def call_fastapi(corridor: str, jam: int, tanggal: str, suhu: float, hujan: bool) -> dict:
    """
    Mengirim request prediksi ke endpoint FastAPI POST /predict.
    Jika API hidup → pakai hasil prediksi ML asli dari model XGBoost/LSTM.
    Jika API mati  → gunakan data sintetis sebagai fallback agar dashboard tetap jalan.
    
    Hasil di-cache selama 60 detik dengan key = kombinasi (corridor, jam, tanggal, suhu, hujan).
    """
    if is_api_alive():
        try:
            # Susun payload JSON sesuai format yang diterima FastAPI
            payload = {
                "koridor": corridor,
                "jam": jam,
                "tanggal": tanggal,
                "suhu": suhu,
                "hujan": int(hujan),  # Konversi True/False ke 1/0
            }
            r = requests.post("http://localhost:8000/predict", json=payload, timeout=1.5)
            if r.status_code == 200:
                return r.json()  # Kembalikan JSON respons dari API
        except Exception:
            pass  # Jika gagal, lanjut ke fallback di bawah

    # ── DATA SINTETIS (FALLBACK) ───────────────────────────────────────────────
    # Digunakan ketika FastAPI tidak berjalan (misal saat development dashboard saja).
    # Menggunakan seed deterministik agar nilai konsisten untuk jam + koridor yang sama.
    np.random.seed(jam + hash(corridor) % 100)
    rush = jam in (7, 8, 9, 17, 18, 19)  # Jam sibuk pagi dan sore
    base = 45 if rush else 22             # Penumpang dasar: 45 saat rush, 22 saat sepi
    noise = np.random.randint(-8, 12)     # Variasi acak ±8-12 penumpang
    weather_penalty = -6 if hujan else 0  # Kurangi 6 penumpang jika hujan
    pred = max(5, base + noise + weather_penalty)  # Minimal 5 penumpang
    cap  = CORRIDORS.get(corridor, {}).get("capacity", 60)
    return {
        "prediksi_penumpang": pred,
        "armada_rekomendasi": max(1, -(-pred // cap)),   # Ceiling division (pembulatan atas)
        "status": get_status(pred, cap),
        "confidence": round(random.uniform(0.72, 0.94), 2),  # Confidence interval 72-94%
        "source": "synthetic",  # Tandai bahwa ini data sintetis, bukan dari API asli
    }


@st.cache_data(ttl=300, show_spinner=False)
def get_bmkg_weather_surabaya() -> dict:
    """
    Mengambil data cuaca real-time dari API BMKG untuk wilayah Surabaya.
    Kita gunakan perkiraan dari endpoint publik BMKG.
    Di-cache 5 menit agar tidak membebani API BMKG.
    """
    try:
        # Gunakan salah satu kode adm4 Surabaya (misal: 35.78.01.1001 untuk area tengah)
        # Jika gagal atau tidak tersedia, fallback ke default
        r = requests.get("https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=35.78.01.1001", timeout=3)
        if r.status_code == 200:
            data = r.json()
            # Ambil data cuaca saat ini dari response
            # Format response BMKG bervariasi, kita ambil data pertama yang relevan (t, weather_desc)
            # Karena ini hanya contoh integrasi P4, kita parse secara sederhana:
            cuaca_data = data.get("data", [])[0].get("cuaca", [[]])[0][0] 
            
            suhu = float(cuaca_data.get("t", 32))
            desc = str(cuaca_data.get("weather_desc", "")).lower()
            hujan = "hujan" in desc or "rain" in desc
            return {"suhu": suhu, "hujan": hujan, "desc": desc}
    except Exception:
        pass
    
    # Fallback jika API BMKG gagal
    return {"suhu": 32.0, "hujan": False, "desc": "cerah (fallback)"}


def generate_24h_forecast(corridor: str, base_jam: int, suhu: float, hujan: bool) -> pd.DataFrame:
    """
    Menghasilkan tabel prediksi penumpang untuk 18 jam operasional (05:00–22:00).
    Memanggil call_fastapi untuk setiap jam.
    Kolom 'Historis' ditambahkan dengan sedikit noise acak sebagai simulasi data historis.
    Hasilnya digunakan untuk menggambar grafik line chart.
    """
    rows = []
    for h in range(5, 23):  # Loop dari jam 05 sampai jam 22
        result = call_fastapi(corridor, h, datetime.now().strftime("%Y-%m-%d"), suhu, hujan)
        cap = CORRIDORS[corridor]["capacity"]
        rows.append({
            "Jam": f"{h:02d}:00",           # Format: "05:00", "06:00", dst.
            "Prediksi": result["prediksi_penumpang"],
            "Historis": max(0, result["prediksi_penumpang"] + np.random.randint(-10, 10)),  # Simulasi data historis
            "Kapasitas": cap,
        })
    return pd.DataFrame(rows)


def log_alert(corridor: str, penumpang: int):
    """
    Mencatat setiap kejadian SURGE ke file CSV (alert_log.csv).
    File dibuat otomatis jika belum ada, lalu baris baru ditambahkan (mode 'append').
    Kolom: timestamp, koridor, penumpang
    """
    log_path = "alert_log.csv"
    exists = os.path.exists(log_path)  # Cek apakah file sudah ada
    with open(log_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "koridor", "penumpang"])
        if not exists:
            writer.writeheader()  # Tulis header kolom hanya jika file baru dibuat
        writer.writerow({
            "timestamp": datetime.now().isoformat(),  # Waktu sekarang dalam format ISO
            "koridor": corridor,
            "penumpang": penumpang,
        })


# =============================================================================
# SESSION STATE — PENYIMPANAN SEMENTARA DI SISI KLIEN
# st.session_state digunakan untuk menyimpan nilai yang perlu bertahan antar
# re-run Streamlit (misalnya waktu refresh terakhir)
# =============================================================================
if "last_refresh" not in st.session_state:
    # Inisialisasi waktu refresh pertama kali saat aplikasi dibuka
    st.session_state.last_refresh = datetime.now()

# =============================================================================
# SIDEBAR — PANEL FILTER DI SEBELAH KIRI
# Berisi semua kontrol input pengguna: koridor, jam, tanggal, cuaca, auto-refresh
# =============================================================================
with st.sidebar:
    st.markdown("### Suroboyo Bus")
    st.markdown("**Smart Demand Dashboard**")
    
    # Statistik dataset yang berhasil di-load
    st.caption(f"📊 {len(CORRIDORS)} koridor aktif · {TOTAL_GEOCODED}/{TOTAL_HALTE} halte berkoordinat")
    st.divider()

    # Dropdown pilih koridor bus
    selected_corridor = st.selectbox("Pilih Koridor", list(CORRIDORS.keys()))
    
    # Slider jam prediksi (05:00 - 22:00), default ke jam sekarang
    selected_jam = st.slider("Jam Prediksi", min_value=5, max_value=22, value=datetime.now().hour or 8, format="%d:00")
    
    # Input tanggal, default ke hari ini
    selected_date = st.date_input("Tanggal", value=datetime.now())

    st.divider()
    st.markdown("**Cuaca (Fitur ML)**")
    
    # Pilihan untuk menggunakan cuaca asli dari BMKG atau simulasi manual
    use_bmkg = st.sidebar.toggle("Gunakan Cuaca Asli (API BMKG)", value=True)
    
    if use_bmkg:
        # Ambil cuaca dari BMKG
        bmkg_data = get_bmkg_weather_surabaya()
        suhu_sim = bmkg_data["suhu"]
        hujan_sim = bmkg_data["hujan"]
        st.info(f"📍 **Surabaya Saat Ini**\n\nSuhu: {suhu_sim}°C\n\nKondisi: {bmkg_data['desc'].title()}")
    else:
        st.caption("Mode Simulasi Aktif")
        # Slider suhu (24°C - 38°C), digunakan sebagai fitur input ke model ML
        suhu_sim = st.slider("Suhu (°C)", 24, 38, 32)
        
        # Toggle hujan — jika aktif, prediksi penumpang akan berkurang
        hujan_sim = st.toggle("Simulasi Hujan", value=False)

    # Toggle auto-refresh — jika aktif, halaman otomatis di-refresh tiap 30 detik
    # Default OFF agar halaman tidak berkedip saat pertama dibuka
    auto_refresh = st.sidebar.toggle("Auto-refresh (30 detik)", value=False)
    
    # Tombol refresh manual — langsung me-rerun halaman saat diklik
    if st.sidebar.button("Refresh Sekarang"):
        st.session_state.last_refresh = datetime.now()
        st.rerun()

    # Tampilkan waktu refresh terakhir
    st.caption(f"Update terakhir: {st.session_state.last_refresh.strftime('%H:%M:%S')}")

# =============================================================================
# HEADER UTAMA — BAGIAN PALING ATAS HALAMAN
# Menampilkan judul dashboard + animasi bus 2D yang berjalan
# =============================================================================
st.markdown(f"""
<div class="main-header" style="position:relative; overflow:hidden; padding-bottom: 5rem;">
    <!-- Teks judul dan subtitle, z-index tinggi agar di atas animasi bus -->
    <div style="position:relative; z-index:1;">
        <h1>Suroboyo Bus &#8212; Smart Demand Dashboard</h1>
        <p>Prediksi permintaan penumpang real-time &bull; Koridor aktif: <b>{selected_corridor.split(':')[0]}</b> &bull; {selected_date.strftime('%A, %d %B %Y')} &bull; {selected_jam:02d}:00 WIB</p>
    </div>
    <!-- Area animasi bus (garis jalan + bus SVG) -->
    <div class="bus-scene">
        <!-- Garis jalan putus-putus yang bergerak ke kiri -->
        <div class="road-line"></div>
        <!-- Bus SVG 2D — digambar menggunakan elemen SVG murni -->
        <svg class="bus-2d" width="200" height="48" viewBox="0 0 200 48" xmlns="http://www.w3.org/2000/svg">
            <!-- Badan utama bus berwarna merah -->
            <rect x="5" y="4" width="185" height="32" rx="6" fill="#dc2626"/>
            <!-- Bagian depan / kabin pengemudi (lebih gelap) -->
            <rect x="162" y="8" width="25" height="24" rx="4" fill="#b91c1c"/>
            <!-- Kaca depan (windshield) berwarna biru muda transparan -->
            <rect x="164" y="10" width="20" height="14" rx="2" fill="#bfdbfe" opacity="0.6"/>
            <!-- Lampu depan berwarna kuning -->
            <rect x="187" y="18" width="6" height="5" rx="1" fill="#fef08a"/>
            <!-- Jendela-jendela penumpang (5 jendela) -->
            <rect x="10" y="9" width="18" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <rect x="33" y="9" width="18" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <rect x="56" y="9" width="18" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <rect x="79" y="9" width="18" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <rect x="102" y="9" width="18" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <rect x="136" y="9" width="22" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <!-- Garis kuning horizontal sebagai aksen desain bus -->
            <rect x="5" y="27" width="185" height="3" fill="#fcd34d" opacity="0.7"/>
            <!-- Aksen segitiga putih di sudut kiri atas (detail desain) -->
            <polygon points="5,4 25,4 5,18" fill="white" opacity="0.08"/>
            <!-- Tulisan nama bus pada badan bawah -->
            <text x="42" y="31" font-family="Arial, sans-serif" font-size="8.5" font-weight="800" fill="white" letter-spacing="1.5" opacity="0.97">SUROBOYO BUS</text>
            <!-- Ban depan dan belakang -->
            <circle cx="38" cy="42" r="7" fill="#1e293b"/>   <!-- Lingkaran luar ban -->
            <circle cx="38" cy="42" r="3" fill="#64748b"/>   <!-- Velg ban depan -->
            <circle cx="155" cy="42" r="7" fill="#1e293b"/>
            <circle cx="155" cy="42" r="3" fill="#64748b"/>  <!-- Velg ban belakang -->
            <!-- Pintu bus di sebelah kiri -->
            <rect x="8" y="16" width="9" height="20" rx="1" fill="#b91c1c"/>
        </svg>
    </div>
</div>
""", unsafe_allow_html=True)

# =============================================================================
# AMBIL PREDIKSI UTAMA — untuk koridor dan jam yang dipilih pengguna
# call_fastapi() memanggil API ML atau fallback ke data sintetis
# =============================================================================
pred_result = call_fastapi(
    selected_corridor,
    selected_jam,
    selected_date.strftime("%Y-%m-%d"),
    suhu_sim,
    hujan_sim,
)
# Ekstrak nilai-nilai dari dictionary hasil prediksi
penumpang    = pred_result["prediksi_penumpang"]   # Jumlah penumpang diprediksi
armada       = pred_result["armada_rekomendasi"]   # Jumlah bus yang direkomendasikan
status       = pred_result["status"]               # "SURGE", "NORMAL", atau "LOW"
confidence   = pred_result["confidence"]           # Nilai kepercayaan model (0.0 - 1.0)
cap          = CORRIDORS[selected_corridor]["capacity"]  # Kapasitas bus koridor ini

# =============================================================================
# ALERT SURGE — Banner merah otomatis muncul jika status = SURGE
# Sekaligus mencatat kejadian SURGE ke file alert_log.csv
# =============================================================================
if status == "SURGE":
    log_alert(selected_corridor, penumpang)  # Catat ke CSV
    st.markdown(f"""
    <div class="surge-banner">
        SURGE ALERT — {selected_corridor.split(':')[1].strip()} pada jam {selected_jam:02d}:00 | 
        Prediksi: {penumpang} penumpang ({int(penumpang/cap*100)}% kapasitas) · Tambah armada segera!
    </div>
    """, unsafe_allow_html=True)

# =============================================================================
# METRIC CARDS — 4 kotak angka di bagian atas konten utama
# Menampilkan prediksi penumpang, armada, kapasitas, dan status
# =============================================================================
c1, c2, c3, c4 = st.columns(4)  # Buat 4 kolom berjajar

# Kolom 1: Prediksi jumlah penumpang
with c1:
    st.markdown(f"""<div class="metric-card">
        <div class="label">Prediksi Penumpang</div>
        <div class="value">{penumpang}</div>
        <div class="delta">{selected_jam:02d}:00 WIB</div>
    </div>""", unsafe_allow_html=True)

# Kolom 2: Jumlah bus yang direkomendasikan
with c2:
    st.markdown(f"""<div class="metric-card">
        <div class="label">Armada Rekomendasi</div>
        <div class="value">{armada}</div>
        <div class="delta">bus diperlukan</div>
    </div>""", unsafe_allow_html=True)

# Kolom 3: Persentase pengisian kapasitas bus
with c3:
    fill_pct = int(penumpang / cap * 100)  # Hitung persentase pengisian
    st.markdown(f"""<div class="metric-card">
        <div class="label">Tingkat Pengisian</div>
        <div class="value">{fill_pct}%</div>
        <div class="delta">Kapasitas bus: {cap} org</div>
    </div>""", unsafe_allow_html=True)

# Kolom 4: Status koridor dengan warna dinamis
with c4:
    # Pilih warna teks sesuai status: merah=SURGE, hijau=NORMAL, biru=LOW
    color_val = "#fca5a5" if status == "SURGE" else "#86efac" if status == "NORMAL" else "#7dd3fc"
    st.markdown(f"""<div class="metric-card">
        <div class="label">Status Koridor</div>
        <div class="value" style="font-size:1.4rem; color:{color_val};">{status}</div>
        <div class="delta">Confidence: {int(confidence*100)}%</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)  # Baris kosong untuk jarak

# =============================================================================
# LAYOUT UTAMA — PETA (kiri) | GRAFIK (kanan)
# Menggunakan 2 kolom dengan lebar yang sama
# =============================================================================
col_map, col_chart = st.columns([1, 1], gap="medium")

# ── PETA GIS FOLIUM ───────────────────────────────────────────────────────────
with col_map:
    st.markdown('<div class="section-title">Peta Halte Suroboyo Bus</div>', unsafe_allow_html=True)

    # Ambil daftar halte untuk koridor yang dipilih
    stops = CORRIDORS[selected_corridor]["stops"]
    
    # Hitung titik tengah peta dari rata-rata koordinat semua halte
    center_lat = np.mean([s[1] for s in stops])
    center_lon = np.mean([s[2] for s in stops])

    # Buat objek peta Folium dengan tile peta CartoDB (tema gelap-terang bersih)
    m = folium.Map(location=[center_lat, center_lon], zoom_start=13,
                   tiles="CartoDB positron")

    # Gambar garis rute bus (PolyLine) menghubungkan semua halte
    coords = [(s[1], s[2]) for s in stops]
    folium.PolyLine(coords, color="#1a237e", weight=3, opacity=0.7).add_to(m)

    # Gambar CircleMarker untuk setiap halte
    for stop_name, lat, lon in stops:
        # Gunakan hasil prediksi utama (lebih cepat daripada memanggil API per halte)
        stop_result = pred_result
        
        # Tambahkan variasi kecil antar halte agar peta lebih informatif
        sp = max(0, stop_result["prediksi_penumpang"] + np.random.randint(-8, 8))
        status_val = get_status(sp, cap)
        
        # Tentukan warna marker berdasarkan status
        color_map = {"SURGE": "red", "NORMAL": "green", "LOW": "blue"}
        color = color_map[status_val]

        # Tambahkan marker lingkaran ke peta
        folium.CircleMarker(
            location=[lat, lon],
            radius=10,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            # Tooltip: muncul saat mouse hover di atas marker
            tooltip=folium.Tooltip(
                f"<b>{stop_name}</b><br>Prediksi: {sp} penumpang<br>Status: {status_val}",
                permanent=False,
            ),
            # Popup: muncul saat marker diklik
            popup=folium.Popup(
                f"<b>{stop_name}</b><br>Penumpang: {sp}<br>Status: {status_val}<br>{lat:.4f}, {lon:.4f}",
                max_width=200,
            ),
        ).add_to(m)

    # Tambahkan legenda status halte di pojok kiri bawah peta
    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;z-index:999;background:white;
         padding:10px 14px;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.15);font-size:12px">
        <b>Status Halte</b><br>
        SURGE (&ge;80%)<br>
        NORMAL<br>
        LOW (&le;40%)
    </div>"""
    m.get_root().html.add_child(folium.Element(legend_html))

    # Render peta ke dalam halaman Streamlit
    # returned_objects=[] PENTING untuk mencegah infinite rerun loop pada streamlit-folium
    st_folium(m, width=700, height=420, returned_objects=[], use_container_width=True)

# ── GRAFIK PREDIKSI 24 JAM ────────────────────────────────────────────────────
with col_chart:
    st.markdown('<div class="section-title">Prediksi Penumpang 24 Jam</div>', unsafe_allow_html=True)

    # Generate dataframe prediksi untuk semua jam operasional
    df_24 = generate_24h_forecast(selected_corridor, selected_jam, suhu_sim, hujan_sim)

    fig = go.Figure()
    
    # Trace 1: Garis data historis (titik-titik abu-abu)
    fig.add_trace(go.Scatter(
        x=df_24["Jam"], y=df_24["Historis"],
        name="Data Historis", mode="lines",
        line=dict(color="#64748b", dash="dot", width=1.5),
    ))
    
    # Trace 2: Garis prediksi ML (garis biru solid dengan titik)
    fig.add_trace(go.Scatter(
        x=df_24["Jam"], y=df_24["Prediksi"],
        name="Prediksi", mode="lines+markers",
        line=dict(color="#60a5fa", width=2.5),
        marker=dict(size=6, color="#60a5fa"),
    ))
    
    # Trace 3: Area bayangan confidence interval (±12% dari prediksi)
    upper = df_24["Prediksi"] * 1.12   # Batas atas: +12%
    lower = df_24["Prediksi"] * 0.88   # Batas bawah: -12%
    fig.add_trace(go.Scatter(
        x=pd.concat([df_24["Jam"], df_24["Jam"][::-1]]),  # Tutup area dengan membalik urutan x
        y=pd.concat([upper, lower[::-1]]),
        fill="toself", fillcolor="rgba(96, 165, 250, 0.15)",
        line=dict(color="rgba(0,0,0,0)"),  # Garis tepi tidak terlihat
        name="Confidence Interval", showlegend=True,
    ))
    
    # Garis horizontal merah putus-putus = batas threshold SURGE (80% kapasitas)
    fig.add_hline(y=cap * 0.8, line_dash="dash", line_color="#ef4444",
                  annotation_text="Threshold SURGE (80%)", annotation_position="top right")

    # Garis vertikal kuning = jam yang sedang dipilih pengguna
    fig.add_trace(go.Scatter(
        x=[f"{selected_jam:02d}:00", f"{selected_jam:02d}:00"],
        y=[0, max(df_24["Prediksi"].max(), cap) * 1.25],
        mode="lines+text",
        line=dict(color="#f59e0b", dash="dot", width=2),
        text=["", "Jam dipilih"],
        textposition="top center",
        textfont=dict(color="#f59e0b", size=10),
        showlegend=False,
        hoverinfo="skip"  # Jangan tampilkan tooltip untuk garis ini
    ))

    # Pengaturan tampilan grafik (layout)
    fig.update_layout(
        height=390,
        margin=dict(t=20, b=40, l=0, r=0),
        xaxis_title="Jam",
        yaxis_title="Penumpang",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color="#f8fafc")),
        paper_bgcolor="rgba(0,0,0,0)",   # Background transparan (mengikuti background halaman)
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(size=11, color="#cbd5e1"),
        xaxis=dict(gridcolor="#334155", zerolinecolor="#334155"),
        yaxis=dict(gridcolor="#334155", zerolinecolor="#334155"),
    )
    fig.update_xaxes(tickangle=-45)  # Miringkan label jam agar tidak bertumpuk
    st.plotly_chart(fig, width="stretch")

st.divider()  # Garis pemisah horizontal

# =============================================================================
# TABEL REKOMENDASI ARMADA — SEMUA KORIDOR
# Menampilkan prediksi dan rekomendasi untuk semua koridor sekaligus
# Diurutkan: SURGE di atas, lalu NORMAL, lalu LOW
# =============================================================================
st.markdown('<div class="section-title">Rekomendasi Armada — Semua Koridor</div>', unsafe_allow_html=True)

# Kumpulkan data prediksi untuk setiap koridor (top 10 terbesar)
# Pastikan koridor yang sedang dipilih selalu termasuk
table_corridors = dict(sorted_corridors[:MAX_HEATMAP_CORRIDORS]) if 'sorted_corridors' in dir() else CORRIDORS
if selected_corridor not in table_corridors:
    table_corridors[selected_corridor] = CORRIDORS[selected_corridor]

table_rows = []
for corridor, info in table_corridors.items():
    res = call_fastapi(corridor, selected_jam, selected_date.strftime("%Y-%m-%d"), suhu_sim, hujan_sim)
    table_rows.append({
        "Koridor": corridor.split(":")[1].strip(),  # Ambil nama setelah titik dua
        "Jam": f"{selected_jam:02d}:00",
        "Prediksi Penumpang": res["prediksi_penumpang"],
        "Armada Rekomendasi": res["armada_rekomendasi"],
        "Pengisian (%)": int(res["prediksi_penumpang"] / info["capacity"] * 100),
        "Status": res["status"],
    })

# Ubah ke DataFrame dan urutkan (SURGE dulu, baru NORMAL, baru LOW)
df_table = pd.DataFrame(table_rows)
df_table = df_table.sort_values("Status", key=lambda x: x.map({"SURGE": 0, "NORMAL": 1, "LOW": 2}))

# Bangun string HTML untuk setiap baris tabel secara manual
# (supaya bisa menyisipkan badge HTML berwarna di kolom Status)
html_rows = ""
for _, row in df_table.iterrows():
    badge = status_badge(row["Status"])  # Dapatkan HTML badge berwarna
    html_rows += f"""<tr>
        <td>{row['Koridor']}</td>
        <td>{row['Jam']}</td>
        <td><b>{row['Prediksi Penumpang']}</b></td>
        <td>{row['Armada Rekomendasi']} bus</td>
        <td>{row['Pengisian (%)']}%</td>
        <td>{badge}</td>
    </tr>"""

# Bungkus dengan HTML tabel lengkap
table_html = f"""
<div class="custom-table-container">
<table class="custom-table">
    <thead>
        <tr>
            <th>Koridor</th>
            <th>Jam</th>
            <th>Prediksi Penumpang</th>
            <th>Armada</th>
            <th>Pengisian</th>
            <th>Status</th>
        </tr>
    </thead>
    <tbody>{html_rows}</tbody>
</table>
</div>"""

st.markdown(table_html, unsafe_allow_html=True)

st.divider()

# =============================================================================
# HEATMAP DEMAND — KORIDOR × JAM
# Visualisasi matriks 2D: sumbu Y = koridor, sumbu X = jam
# Warna menunjukkan intensitas penumpang (biru=sepi → kuning→merah=ramai)
# =============================================================================
st.markdown('<div class="section-title">Heatmap Demand — Koridor × Jam</div>', unsafe_allow_html=True)

# Bangun matriks data: tiap elemen = prediksi penumpang [koridor][jam]
# Batasi ke 10 koridor terbesar (berdasarkan jumlah halte) agar heatmap tetap responsif
MAX_HEATMAP_CORRIDORS = 10
sorted_corridors = sorted(CORRIDORS.items(), key=lambda x: len(x[1]["stops"]), reverse=True)
heatmap_corridors = dict(sorted_corridors[:MAX_HEATMAP_CORRIDORS])

heatmap_data = []
corridor_labels = [c.split(":")[1].strip() for c in heatmap_corridors.keys()]  # Label nama koridor
for corridor, info in heatmap_corridors.items():
    row = []
    for h in range(5, 23):  # Untuk setiap jam operasional
        res = call_fastapi(corridor, h, selected_date.strftime("%Y-%m-%d"), suhu_sim, hujan_sim)
        row.append(res["prediksi_penumpang"])
    heatmap_data.append(row)

# Buat heatmap menggunakan Plotly go.Heatmap
fig_heat = go.Figure(data=go.Heatmap(
    z=heatmap_data,
    x=[f"{h:02d}:00" for h in range(5, 23)],  # Label jam di sumbu X
    y=corridor_labels,                          # Label koridor di sumbu Y
    # Skala warna: gelap=sepi, biru=normal, kuning=padat, merah=SURGE
    colorscale=[
        [0.0,  "#0f172a"],
        [0.4,  "#3b82f6"],
        [0.7,  "#f59e0b"],
        [1.0,  "#ef4444"],
    ],
    colorbar=dict(title="Penumpang", tickfont=dict(color="#f8fafc")),
    hoverongaps=False,
    hovertemplate="Koridor: %{y}<br>Jam: %{x}<br>Penumpang: %{z}<extra></extra>",
))
heatmap_height = max(220, len(heatmap_corridors) * 35)  # Tinggi dinamis berdasarkan jumlah koridor
fig_heat.update_layout(
    height=heatmap_height,
    margin=dict(t=10, b=40, l=0, r=0),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    xaxis_title="Jam",
    font=dict(size=11, color="#cbd5e1"),
)
fig_heat.update_xaxes(tickangle=-45)
st.plotly_chart(fig_heat, width="stretch")

# =============================================================================
# AUTO-REFRESH — Refresh otomatis setiap 30 detik
# Hanya aktif jika toggle "Auto-refresh" di sidebar dinyalakan
# time.sleep(30) membekukan halaman 30 detik lalu st.rerun() me-refresh ulang
# =============================================================================
if auto_refresh:
    time.sleep(30)  # Tunggu 30 detik
    st.session_state.last_refresh = datetime.now()  # Update waktu refresh
    st.rerun()  # Paksa Streamlit menjalankan ulang seluruh skrip dari atas
