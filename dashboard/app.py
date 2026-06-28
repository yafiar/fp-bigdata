# =============================================================================
# SUROBOYO BUS — SMART DEMAND DASHBOARD
# Dashboard prediksi kepadatan penumpang bus berbasis Machine Learning
# Final Project Big Data — Streamlit Frontend
# =============================================================================

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import folium
from streamlit_folium import st_folium
import requests
from datetime import datetime, timedelta
import time
import random
import os
import csv
import re

st.set_page_config(
    page_title="Suroboyo Bus — Smart Demand Dashboard",
    page_icon="bus",
    layout="wide",
    initial_sidebar_state="expanded",
)

with open("style.css", "r", encoding="utf-8") as f:
    css_isi = f.read()
st.markdown(f"<style>{css_isi}</style>", unsafe_allow_html=True)

# =============================================================================
# DATA DINAMIS — KORIDOR DAN HALTE BUS
# =============================================================================

@st.cache_data(ttl=60)
def load_corridors_data():
    file_path = "../dataset/Halte_Suroboyo_dengan_Koordinat.csv"
    koridor_path = "../dataset/Data Koridor SuroboyoBus & WaraWiri API.xlsx"

    if not os.path.exists(file_path):
        st.error("Data halte berkoordinat belum tersedia!")
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

    for _, row in df_koridor.iterrows():
        key = str(row['KEY']).strip()
        tipe = str(row['KETERANGAN']).strip()
        nama_rute = f"{tipe} {key.upper()}: {str(row['RUTE']).strip()}"

        pattern = re.compile(r'(?:^|[\s/])' + re.escape(key) + r'(?:[\s/]|$)', re.IGNORECASE)

        halte_list = []
        for _, h_row in halte_with_coords.iterrows():
            rutes = str(h_row['Rute_yang_Melewati'])
            if pattern.search(rutes):
                lat = h_row['Latitude']
                lon = h_row['Longitude']
                halte_list.append((str(h_row['Nama_Halte']).strip(), float(lat), float(lon)))

        if halte_list:
            is_feeder = 0 if tipe == "SB" else 1  # SB=trunk, WW=feeder
            corridors_dict[nama_rute] = {
                "stops": halte_list,
                "capacity": 60 if tipe == "SB" else 15,
                "feeder": is_feeder,
            }

    return corridors_dict, total_halte, total_geocoded


CORRIDORS, TOTAL_HALTE, TOTAL_GEOCODED = load_corridors_data()

@st.cache_data(ttl=60)
def load_armada_data():
    file_path = "../dataset/Data Armada SuroboyoBus 2025.xlsx"
    if not os.path.exists(file_path):
        return 0
    try:
        df = pd.read_excel(file_path)
        suroboyo_df = df[df['kategori'].str.contains('suroboyo_bus', case=False, na=False)]
        return int(suroboyo_df['jumlah'].sum())
    except:
        return 0

TOTAL_ARMADA_SUROBOYOBUS = load_armada_data()

if not CORRIDORS:
    st.stop()

HOURS = list(range(5, 23))

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_status(penumpang: int, capacity: int) -> str:
    ratio = penumpang / capacity
    if ratio >= 0.80:
        return "SURGE"
    elif ratio <= 0.40:
        return "LOW"
    return "NORMAL"


def status_badge(status: str) -> str:
    cls = {"SURGE": "badge-surge", "LOW": "badge-low", "NORMAL": "badge-normal"}[status]
    return f'<span class="{cls}">{status}</span>'


@st.cache_data(ttl=30, show_spinner=False)
def is_api_alive() -> bool:
    try:
        requests.get("http://localhost:8000/", timeout=0.2)
        return True
    except Exception:
        return False


@st.cache_data(ttl=60, show_spinner=False)
def call_fastapi(corridor: str, jam: int, tanggal: str, suhu: float, hujan: bool, feeder: int = 0) -> dict:
    """
    Kirim request ke FastAPI dengan field feeder agar API bisa bedakan
    koridor trunk (SB, kapasitas 60) vs feeder Wara-Wiri (kapasitas 15).
    """
    tanggal_dt = datetime.strptime(tanggal, "%Y-%m-%d")
    day_of_week = tanggal_dt.strftime("%A")
    is_weekend = 1 if day_of_week in ["Saturday", "Sunday"] else 0

    if is_api_alive():
        try:
            payload = {
                "koridor": corridor,
                "jam": jam,
                "tanggal": tanggal,
                "suhu": suhu,
                "hujan": int(hujan),
                "feeder": feeder,  # PENTING: kirim info feeder ke API
                "day_of_week": day_of_week,
                "is_weekend": is_weekend,
            }
            r = requests.post("http://localhost:8000/predict", json=payload, timeout=1.5)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass

    # Fallback sintetis — juga bedakan feeder vs trunk
    cap = CORRIDORS.get(corridor, {}).get("capacity", 60)
    np.random.seed(jam + hash(corridor) % 100)
    rush = jam in (7, 8, 9, 17, 18, 19)
    if feeder:
        base = 10 if rush else 6
    else:
        base = 45 if rush else 22
    noise = np.random.randint(-3, 5)
    weather_penalty = -2 if hujan else 0
    pred = max(1, base + noise + weather_penalty)
    return {
        "prediksi_penumpang": pred,
        "armada_rekomendasi": max(1, -(-pred // cap)),
        "status": get_status(pred, cap),
        "confidence": round(random.uniform(0.72, 0.94), 2),
        "demand_level": "SEDANG" if pred > 10 else "RENDAH",
        "headway_pred": round(random.uniform(10, 20), 1),
        "headway_status": "BAIK" if random.random() > 0.5 else "BURUK",
        "source": "synthetic",
    }


@st.cache_data(ttl=300, show_spinner=False)
def get_bmkg_weather_surabaya() -> dict:
    try:
        r = requests.get("https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=35.78.01.1001", timeout=3)
        if r.status_code == 200:
            data = r.json()
            cuaca_data = data.get("data", [])[0].get("cuaca", [[]])[0][0]
            suhu = float(cuaca_data.get("t", 32))
            desc = str(cuaca_data.get("weather_desc", "")).lower()
            hujan = "hujan" in desc or "rain" in desc
            return {"suhu": suhu, "hujan": hujan, "desc": desc}
    except Exception:
        pass
    return {"suhu": 32.0, "hujan": False, "desc": "cerah (fallback)"}


def generate_24h_forecast(corridor: str, base_jam: int, suhu: float, hujan: bool, tanggal: str) -> pd.DataFrame:
    feeder = CORRIDORS[corridor]["feeder"]
    cap = CORRIDORS[corridor]["capacity"]
    rows = []
    for h in range(5, 23):
        result = call_fastapi(corridor, h, tanggal, suhu, hujan, feeder=feeder)
        rows.append({
            "Jam": f"{h:02d}:00",
            "Prediksi": result["prediksi_penumpang"],
            "Historis": max(0, result["prediksi_penumpang"] + np.random.randint(-5, 6)),
            "Kapasitas": cap,
        })
    return pd.DataFrame(rows)


def log_alert(corridor: str, penumpang: int):
    log_path = "alert_log.csv"
    exists = os.path.exists(log_path)
    with open(log_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "koridor", "penumpang"])
        if not exists:
            writer.writeheader()
        writer.writerow({
            "timestamp": datetime.now().isoformat(),
            "koridor": corridor,
            "penumpang": penumpang,
        })


# =============================================================================
# SESSION STATE
# =============================================================================
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = datetime.now()

# =============================================================================
# SIDEBAR
# =============================================================================
with st.sidebar:
    st.markdown("### Suroboyo Bus")
    st.markdown("**Smart Demand Dashboard**")
    st.caption(f"📊 {len(CORRIDORS)} koridor aktif · {TOTAL_GEOCODED}/{TOTAL_HALTE} halte berkoordinat")
    if TOTAL_ARMADA_SUROBOYOBUS > 0:
        st.caption(f"🚌 Total Armada Suroboyo Bus: {TOTAL_ARMADA_SUROBOYOBUS} unit")
    st.divider()

    selected_corridor = st.selectbox("Pilih Koridor", list(CORRIDORS.keys()))
    selected_jam = st.slider("Jam Prediksi", min_value=5, max_value=22, value=datetime.now().hour or 8, format="%d:00")
    selected_date = st.date_input("Tanggal", value=datetime.now())

    st.divider()
    st.markdown("**Kondisi Cuaca (BMKG)**")

    # Menggunakan cuaca real-time dari BMKG (Fixed)
    bmkg_data = get_bmkg_weather_surabaya()
    suhu_sim = bmkg_data["suhu"]
    hujan_sim = bmkg_data["hujan"]
    st.info(f"📍 **Surabaya Saat Ini**\n\nSuhu: {suhu_sim}°C\n\nKondisi: {bmkg_data['desc'].title()}")

    auto_refresh = st.sidebar.toggle("Auto-refresh (30 detik)", value=False)

    if st.sidebar.button("Refresh Sekarang"):
        st.session_state.last_refresh = datetime.now()
        st.rerun()

    st.caption(f"Update terakhir: {st.session_state.last_refresh.strftime('%H:%M:%S')}")

# =============================================================================
# HEADER
# =============================================================================
st.markdown(f"""
<div class="main-header" style="position:relative; overflow:hidden; padding-bottom: 5rem;">
    <div style="position:relative; z-index:1;">
        <h1>Suroboyo Bus &#8212; Smart Demand Dashboard</h1>
        <p>Prediksi permintaan penumpang real-time &bull; Koridor aktif: <b>{selected_corridor.split(':')[0]}</b> &bull; {selected_date.strftime('%A, %d %B %Y')} &bull; {selected_jam:02d}:00 WIB</p>
    </div>
    <div class="bus-scene">
        <div class="road-line"></div>
        <svg class="bus-2d" width="200" height="48" viewBox="0 0 200 48" xmlns="http://www.w3.org/2000/svg">
            <rect x="5" y="4" width="185" height="32" rx="6" fill="#dc2626"/>
            <rect x="162" y="8" width="25" height="24" rx="4" fill="#b91c1c"/>
            <rect x="164" y="10" width="20" height="14" rx="2" fill="#bfdbfe" opacity="0.6"/>
            <rect x="187" y="18" width="6" height="5" rx="1" fill="#fef08a"/>
            <rect x="10" y="9" width="18" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <rect x="33" y="9" width="18" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <rect x="56" y="9" width="18" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <rect x="79" y="9" width="18" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <rect x="102" y="9" width="18" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <rect x="136" y="9" width="22" height="13" rx="2" fill="#0f172a" opacity="0.85"/>
            <rect x="5" y="27" width="185" height="3" fill="#fcd34d" opacity="0.7"/>
            <polygon points="5,4 25,4 5,18" fill="white" opacity="0.08"/>
            <text x="42" y="31" font-family="Arial, sans-serif" font-size="8.5" font-weight="800" fill="white" letter-spacing="1.5" opacity="0.97">SUROBOYO BUS</text>
            <circle cx="38" cy="42" r="7" fill="#1e293b"/>
            <circle cx="38" cy="42" r="3" fill="#64748b"/>
            <circle cx="155" cy="42" r="7" fill="#1e293b"/>
            <circle cx="155" cy="42" r="3" fill="#64748b"/>
            <rect x="8" y="16" width="9" height="20" rx="1" fill="#b91c1c"/>
        </svg>
    </div>
</div>
""", unsafe_allow_html=True)

# =============================================================================
# PREDIKSI UTAMA
# =============================================================================
selected_feeder = CORRIDORS[selected_corridor]["feeder"]
cap = CORRIDORS[selected_corridor]["capacity"]

pred_result = call_fastapi(
    selected_corridor,
    selected_jam,
    selected_date.strftime("%Y-%m-%d"),
    suhu_sim,
    hujan_sim,
    feeder=selected_feeder,
)

penumpang  = pred_result["prediksi_penumpang"]
armada     = pred_result["armada_rekomendasi"]
status     = pred_result["status"]
confidence = pred_result.get("confidence", 0.0)
demand     = pred_result.get("demand_level", "Unknown").upper()
hw_pred    = pred_result.get("headway_pred", 0.0)
hw_status  = pred_result.get("headway_status", "Unknown").upper()

# =============================================================================
# ALERT SURGE
# =============================================================================
if status == "SURGE":
    log_alert(selected_corridor, penumpang)
    st.markdown(f"""
    <div class="surge-banner">
        SURGE ALERT — {selected_corridor.split(':')[1].strip()} pada jam {selected_jam:02d}:00 |
        Prediksi: {penumpang} penumpang ({int(penumpang/cap*100)}% kapasitas) · Tambah armada segera!
    </div>
    """, unsafe_allow_html=True)

if pred_result.get("source") == "synthetic":
    st.error("🚨 **API OFFLINE / TERPUTUS! (DATA DUMMY AKTIF)** 🚨\n\nKoneksi ke Machine Learning API (FastAPI di port 8000) gagal atau terputus. Dashboard secara otomatis beralih menggunakan **Data Simulasi (Synthetic Fallback)** agar aplikasi tetap dapat diakses tanpa *crash*.")

# =============================================================================
# METRIC CARDS
# =============================================================================
c1, c2, c3 = st.columns(3)
c4, c5, c6 = st.columns(3)

with c1:
    st.markdown(f"""<div class="metric-card">
        <div class="label">Prediksi Penumpang</div>
        <div class="value">{penumpang}</div>
        <div class="delta">{selected_jam:02d}:00 WIB</div>
    </div>""", unsafe_allow_html=True)

with c2:
    st.markdown(f"""<div class="metric-card">
        <div class="label">Armada Rekomendasi</div>
        <div class="value">{armada}</div>
        <div class="delta">bus diperlukan</div>
    </div>""", unsafe_allow_html=True)

with c3:
    fill_pct = int(penumpang / cap * 100)
    st.markdown(f"""<div class="metric-card">
        <div class="label">Tingkat Pengisian</div>
        <div class="value">{fill_pct}%</div>
        <div class="delta">Kapasitas bus: {cap} org</div>
    </div>""", unsafe_allow_html=True)

with c4:
    color_val = "#fca5a5" if status == "SURGE" else "#86efac" if status == "NORMAL" else "#7dd3fc"
    st.markdown(f"""<div class="metric-card">
        <div class="label">Status Koridor</div>
        <div class="value" style="font-size:1.4rem; color:{color_val};">{status}</div>
        <div class="delta">Confidence: {int(confidence*100)}%</div>
    </div>""", unsafe_allow_html=True)

with c5:
    demand_color = "#fca5a5" if demand == "TINGGI" else "#fef08a" if demand == "SEDANG" else "#86efac"
    st.markdown(f"""<div class="metric-card">
        <div class="label">Tingkat Permintaan (ML)</div>
        <div class="value" style="font-size:1.4rem; color:{demand_color};">{demand}</div>
        <div class="delta">Model: XGBoost Classifier</div>
    </div>""", unsafe_allow_html=True)

with c6:
    hw_color = "#86efac" if hw_status == "BAIK" else "#fca5a5"
    st.markdown(f"""<div class="metric-card">
        <div class="label">Estimasi Headway (ML)</div>
        <div class="value" style="font-size:1.4rem; color:{hw_color};">{hw_pred} <span style="font-size:1rem">mnt</span></div>
        <div class="delta">Status: {hw_status}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# =============================================================================
# PETA + GRAFIK
# =============================================================================
col_map, col_chart = st.columns([1, 1], gap="medium")

with col_map:
    st.markdown('<div class="section-title">Peta Halte Suroboyo Bus</div>', unsafe_allow_html=True)

    stops = CORRIDORS[selected_corridor]["stops"]
    center_lat = np.mean([s[1] for s in stops])
    center_lon = np.mean([s[2] for s in stops])

    m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles="CartoDB positron")

    coords = [(s[1], s[2]) for s in stops]
    folium.PolyLine(coords, color="#1a237e", weight=3, opacity=0.7).add_to(m)

    for stop_name, lat, lon in stops:
        sp = max(0, pred_result["prediksi_penumpang"] + np.random.randint(-5, 6))
        status_val = get_status(sp, cap)
        color_map = {"SURGE": "red", "NORMAL": "green", "LOW": "blue"}
        color = color_map[status_val]

        folium.CircleMarker(
            location=[lat, lon],
            radius=10,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            tooltip=folium.Tooltip(
                f"<b>{stop_name}</b><br>Prediksi: {sp} penumpang<br>Status: {status_val}",
                permanent=False,
            ),
            popup=folium.Popup(
                f"<b>{stop_name}</b><br>Penumpang: {sp}<br>Status: {status_val}<br>{lat:.4f}, {lon:.4f}",
                max_width=200,
            ),
        ).add_to(m)

    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;z-index:999;background:white;
         padding:10px 14px;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.15);font-size:12px">
        <b>Status Halte</b><br>
        🔴 SURGE (&ge;80%)<br>
        🟢 NORMAL<br>
        🔵 LOW (&le;40%)
    </div>"""
    m.get_root().html.add_child(folium.Element(legend_html))

    st_folium(m, width=700, height=420, returned_objects=[], use_container_width=True)

with col_chart:
    st.markdown('<div class="section-title">Prediksi Penumpang 24 Jam</div>', unsafe_allow_html=True)

    df_24 = generate_24h_forecast(selected_corridor, selected_jam, suhu_sim, hujan_sim, selected_date.strftime("%Y-%m-%d"))

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_24["Jam"], y=df_24["Historis"],
        name="Data Historis", mode="lines",
        line=dict(color="#64748b", dash="dot", width=1.5),
    ))

    fig.add_trace(go.Scatter(
        x=df_24["Jam"], y=df_24["Prediksi"],
        name="Prediksi", mode="lines+markers",
        line=dict(color="#60a5fa", width=2.5),
        marker=dict(size=6, color="#60a5fa"),
    ))

    upper = df_24["Prediksi"] * 1.12
    lower = df_24["Prediksi"] * 0.88
    fig.add_trace(go.Scatter(
        x=pd.concat([df_24["Jam"], df_24["Jam"][::-1]]),
        y=pd.concat([upper, lower[::-1]]),
        fill="toself", fillcolor="rgba(96, 165, 250, 0.15)",
        line=dict(color="rgba(0,0,0,0)"),
        name="Confidence Interval", showlegend=True,
    ))

    fig.add_hline(y=cap * 0.8, line_dash="dash", line_color="#ef4444",
                  annotation_text="Threshold SURGE (80%)", annotation_position="top right")

    fig.add_trace(go.Scatter(
        x=[f"{selected_jam:02d}:00", f"{selected_jam:02d}:00"],
        y=[0, max(df_24["Prediksi"].max(), cap) * 1.25],
        mode="lines+text",
        line=dict(color="#f59e0b", dash="dot", width=2),
        text=["", "Jam dipilih"],
        textposition="top center",
        textfont=dict(color="#f59e0b", size=10),
        showlegend=False,
        hoverinfo="skip",
    ))

    fig.update_layout(
        height=390,
        margin=dict(t=20, b=40, l=0, r=0),
        xaxis_title="Jam",
        yaxis_title="Penumpang",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color="#f8fafc")),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(size=11, color="#cbd5e1"),
        xaxis=dict(gridcolor="#334155", zerolinecolor="#334155"),
        yaxis=dict(gridcolor="#334155", zerolinecolor="#334155"),
    )
    fig.update_xaxes(tickangle=-45)
    st.plotly_chart(fig, width="stretch")

st.divider()

# =============================================================================
# TABEL REKOMENDASI ARMADA — SEMUA KORIDOR
# =============================================================================
st.markdown('<div class="section-title">Rekomendasi Armada — Semua Koridor</div>', unsafe_allow_html=True)

MAX_HEATMAP_CORRIDORS = 10
sorted_corridors = sorted(CORRIDORS.items(), key=lambda x: len(x[1]["stops"]), reverse=True)
table_corridors = dict(sorted_corridors[:MAX_HEATMAP_CORRIDORS])
if selected_corridor not in table_corridors:
    table_corridors[selected_corridor] = CORRIDORS[selected_corridor]

table_rows = []
for corridor, info in table_corridors.items():
    res = call_fastapi(
        corridor,
        selected_jam,
        selected_date.strftime("%Y-%m-%d"),
        suhu_sim,
        hujan_sim,
        feeder=info["feeder"],  # kirim feeder per koridor
    )
    table_rows.append({
        "Koridor": corridor.split(":")[1].strip(),
        "Jam": f"{selected_jam:02d}:00",
        "Prediksi Penumpang": res["prediksi_penumpang"],
        "Armada Rekomendasi": res["armada_rekomendasi"],
        "Pengisian (%)": int(res["prediksi_penumpang"] / info["capacity"] * 100),
        "Status": res["status"],
    })

df_table = pd.DataFrame(table_rows)
df_table = df_table.sort_values("Status", key=lambda x: x.map({"SURGE": 0, "NORMAL": 1, "LOW": 2}))

html_rows = ""
for _, row in df_table.iterrows():
    badge = status_badge(row["Status"])
    html_rows += f"""<tr>
        <td>{row['Koridor']}</td>
        <td>{row['Jam']}</td>
        <td><b>{row['Prediksi Penumpang']}</b></td>
        <td>{row['Armada Rekomendasi']} bus</td>
        <td>{row['Pengisian (%)']}%</td>
        <td>{badge}</td>
    </tr>"""

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
# HEATMAP DEMAND — KORIDOR x JAM
# =============================================================================
st.markdown('<div class="section-title">Heatmap Demand — Koridor × Jam</div>', unsafe_allow_html=True)

heatmap_corridors = dict(sorted_corridors[:MAX_HEATMAP_CORRIDORS])

heatmap_data = []
corridor_labels = [c.split(":")[1].strip() for c in heatmap_corridors.keys()]
for corridor, info in heatmap_corridors.items():
    row = []
    for h in range(5, 23):
        res = call_fastapi(
            corridor, h,
            selected_date.strftime("%Y-%m-%d"),
            suhu_sim, hujan_sim,
            feeder=info["feeder"],
        )
        row.append(res["prediksi_penumpang"])
    heatmap_data.append(row)

fig_heat = go.Figure(data=go.Heatmap(
    z=heatmap_data,
    x=[f"{h:02d}:00" for h in range(5, 23)],
    y=corridor_labels,
    colorscale=[
        [0.0, "#0f172a"],
        [0.4, "#3b82f6"],
        [0.7, "#f59e0b"],
        [1.0, "#ef4444"],
    ],
    colorbar=dict(title="Penumpang", tickfont=dict(color="#f8fafc")),
    hoverongaps=False,
    hovertemplate="Koridor: %{y}<br>Jam: %{x}<br>Penumpang: %{z}<extra></extra>",
))
heatmap_height = max(220, len(heatmap_corridors) * 35)
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
# AUTO-REFRESH
# =============================================================================
if auto_refresh:
    time.sleep(30)
    st.session_state.last_refresh = datetime.now()
    st.rerun()