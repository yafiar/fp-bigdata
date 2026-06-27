"""
P3 — FastAPI Inference API
===========================
Endpoint: POST /predict
Input : { koridor, jam, suhu, hujan, [opsional: n_total, feeder, day_of_week] }
Output: { prediksi_penumpang, armada_rekomendasi, status, confidence, demand_level, headway_pred }

Jalankan: uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
Docs    : http://localhost:8000/docs
"""

import os, json, pickle, math
import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────
# LOAD MODEL
# ─────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.dirname(__file__))  # direktori ml/
MODEL_DIR  = os.path.join(BASE_DIR, "models")

def load_pkl(filename):
    path = os.path.join(MODEL_DIR, filename)
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return pickle.load(f)

# Load semua model (bisa None jika belum ditraining)
clf_model      = load_pkl("xgboost_model.pkl")
label_encoders = load_pkl("label_encoder.pkl")
reg_hw_model   = load_pkl("xgb_regressor_headway.pkl")
reg_ne_model   = load_pkl("xgb_regressor_nefektif.pkl")
reg_encoders   = load_pkl("label_encoders_regressor.pkl")

# Load metadata fitur
feature_path = os.path.join(MODEL_DIR, "feature_columns.json")
reg_feat_path = os.path.join(MODEL_DIR, "regressor_feature_columns.json")

feat_meta = json.load(open(feature_path)) if os.path.exists(feature_path) else {}
reg_meta  = json.load(open(reg_feat_path)) if os.path.exists(reg_feat_path) else {}

MODEL_LOADED = clf_model is not None

# ─────────────────────────────────────────────────────────
# KONSTANTA SUROBOYO BUS
# ─────────────────────────────────────────────────────────
CAPACITY_SB  = 60   # kapasitas Suroboyo Bus (articulated)
CAPACITY_WW  = 15   # kapasitas Wara-Wiri
SPM_HEADWAY  = 15   # standar pelayanan minimal headway (menit)

# Mapping jam → time_category (sama dengan P5)
def get_time_category(hour: int) -> str:
    if 0 <= hour < 6:   return "dini_hari"
    if 6 <= hour < 9:   return "pagi_peak"
    if 9 <= hour < 12:  return "siang"
    if 12 <= hour < 16: return "siang"
    if 16 <= hour < 20: return "sore_peak"
    if 20 <= hour < 24: return "malam"
    return "siang"

def get_status(n_pred: int, capacity: int) -> str:
    ratio = n_pred / capacity if capacity > 0 else 0
    if ratio > 0.8:  return "SURGE"
    if ratio < 0.4:  return "LOW"
    return "NORMAL"

# ─────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────
app = FastAPI(
    title="SuroboyoBus — Demand Prediction API",
    description=(
        "P3 — Machine Learning Inference API\n\n"
        "Memprediksi demand penumpang dan headway SuroboyoBus "
        "berdasarkan waktu, koridor, dan kondisi cuaca."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────────────────
class PredictRequest(BaseModel):
    koridor:     str   = Field(..., description="Kode koridor, misal '1', '12', 'fd05'")
    jam:         int   = Field(..., ge=0, le=23, description="Jam (0-23)")
    tanggal:     Optional[str]   = Field(None, description="Tanggal YYYY-MM-DD (opsional)")
    suhu:        Optional[float] = Field(None, description="Suhu udara °C (opsional)")
    hujan:       Optional[int]   = Field(0, description="0=tidak hujan, 1=hujan")
    n_total:     Optional[int]   = Field(None, description="Total bus di koridor (default: estimasi)")
    feeder:      Optional[int]   = Field(0, description="0=trunk (SB), 1=feeder (WW)")
    day_of_week: Optional[str]   = Field(None, description="Nama hari (Monday...Sunday)")
    is_weekend:  Optional[int]   = Field(None, description="0=weekday, 1=weekend")

class PredictResponse(BaseModel):
    prediksi_penumpang:  int
    armada_rekomendasi:  int
    status:              str
    confidence:          float
    demand_level:        str
    headway_pred:        Optional[float]
    headway_status:      str
    source:              str

# ─────────────────────────────────────────────────────────
# HELPER: BUILD FEATURE VECTOR
# ─────────────────────────────────────────────────────────
def build_feature_vector(req: PredictRequest, for_regressor: bool = False):
    """
    Bangun feature vector dari request.
    Kolom mengikuti urutan yang dipakai saat training (feature_columns.json).
    """
    hour    = req.jam
    feeder  = req.feeder or 0
    is_peak = 1 if hour in list(range(6, 10)) + list(range(16, 20)) else 0

    # is_weekend dari request atau inferensikan dari day_of_week
    if req.is_weekend is not None:
        is_weekend = req.is_weekend
    elif req.day_of_week:
        is_weekend = 1 if req.day_of_week in ["Saturday", "Sunday"] else 0
    else:
        is_weekend = 0  # default weekday

    # n_total: pakai dari request atau estimasi dari kapasitas armada koridor
    n_total = req.n_total if req.n_total is not None else (8 if feeder else 12)

    # Estimasi n_efektif dan pct_efektif (heuristic berbasis jam)
    # Jam peak → lebih banyak bus aktif
    pct_efektif_est = 0.85 if is_peak else 0.70
    n_efektif_est   = max(1, int(n_total * pct_efektif_est))
    pct_efektif     = round(n_efektif_est / n_total * 100, 1) if n_total > 0 else 70.0

    # Headway estimate heuristic (pakai saat fitur regressor belum tersedia)
    cycle = 60 if feeder else 120
    hw_est = round(cycle / n_efektif_est, 1)

    # service_level heuristic
    if hw_est <= 15:   svc = "baik"
    elif hw_est <= 25: svc = "sedang"
    else:              svc = "buruk"

    time_cat = get_time_category(hour)
    pct_mangkal = 15.0 if not is_peak else 5.0

    # avg_speed heuristic (km/h)
    avg_speed = 18.0 if is_peak else 25.0

    if for_regressor:
        # Encode kategorik untuk regressor
        enc = reg_encoders or {}
        le_time = enc.get("time_category")
        le_svc  = enc.get("service_level")

        tc_enc = 0
        sv_enc = 0
        if le_time and time_cat in le_time.classes_:
            tc_enc = int(le_time.transform([time_cat])[0])
        if le_svc and svc in le_svc.classes_:
            sv_enc = int(le_svc.transform([svc])[0])

        fv = [
            hour, is_peak, is_weekend, feeder,
            n_total, pct_efektif, avg_speed, pct_mangkal,
            tc_enc, sv_enc,
        ]
        return fv

    else:
        # Encode untuk classifier
        enc = label_encoders or {}
        le_time = enc.get("time_category")
        le_svc  = enc.get("service_level")
        headway_gap = round(hw_est - SPM_HEADWAY, 1)

        tc_enc = 0
        sv_enc = 0
        if le_time and time_cat in le_time.classes_:
            tc_enc = int(le_time.transform([time_cat])[0])
        if le_svc and svc in le_svc.classes_:
            sv_enc = int(le_svc.transform([svc])[0])

        fv = [
            hour, is_peak, is_weekend, feeder,
            n_total, n_efektif_est, pct_efektif,
            hw_est, headway_gap,
            avg_speed, pct_mangkal,
            tc_enc, sv_enc,
        ]
        return fv, n_efektif_est, hw_est, svc


# ─────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "service": "SuroboyoBus Demand Prediction API",
        "version": "1.0.0",
        "model_loaded": MODEL_LOADED,
        "endpoints": ["/predict", "/health", "/docs"],
    }

@app.get("/health")
def health():
    return {
        "status": "ok",
        "classifier_loaded":  clf_model is not None,
        "hw_regressor_loaded": reg_hw_model is not None,
        "ne_regressor_loaded": reg_ne_model is not None,
    }

@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    feeder   = req.feeder or 0
    capacity = CAPACITY_WW if feeder else CAPACITY_SB
    hour     = req.jam

    # ── Coba pakai model ML ─────────────────────────────
    if MODEL_LOADED and label_encoders:
        try:
            fv, n_efektif_est, hw_est, svc_est = build_feature_vector(req, for_regressor=False)
            fv_arr = np.array(fv, dtype=float).reshape(1, -1)

            # Prediksi demand_level (classifier)
            y_pred   = clf_model.predict(fv_arr)[0]
            proba    = clf_model.predict_proba(fv_arr)[0]
            le_tgt   = label_encoders["target"]
            demand   = le_tgt.inverse_transform([y_pred])[0]
            confidence = float(max(proba))

            # Prediksi headway & n_efektif (regressor, jika tersedia)
            hw_pred = None
            ne_pred = n_efektif_est
            if reg_hw_model and reg_ne_model:
                fv_reg = np.array(
                    build_feature_vector(req, for_regressor=True), dtype=float
                ).reshape(1, -1)
                hw_pred = round(float(reg_hw_model.predict(fv_reg)[0]), 1)
                ne_pred = max(1, int(round(float(reg_ne_model.predict(fv_reg)[0]))))

            # Konversi demand_level ke estimasi penumpang per koridor per jam
            # Trunk SB (kapasitas 60) vs Feeder Wara-Wiri (kapasitas 15)
            is_peak = 1 if hour in list(range(6, 10)) + list(range(16, 20)) else 0
            if feeder:
                demand_to_pax = {
                    "tinggi": 13 if is_peak else 10,
                    "sedang":  9 if is_peak else  7,
                    "rendah":  5 if is_peak else  4,
                }
            else:
                demand_to_pax = {
                    "tinggi": 52 if is_peak else 44,
                    "sedang": 36 if is_peak else 28,
                    "rendah": 18 if is_peak else 12,
                }
            pax_pred = demand_to_pax.get(demand, capacity // 2)
            if req.hujan:
                pax_pred = int(pax_pred * 0.85)

            armada_rek = max(1, math.ceil(pax_pred / capacity))
            status     = get_status(pax_pred, capacity)
            hw_status  = "BAIK" if (hw_pred or hw_est) <= SPM_HEADWAY else "BURUK"

            return PredictResponse(
                prediksi_penumpang=pax_pred,
                armada_rekomendasi=armada_rek,
                status=status,
                confidence=round(confidence, 3),
                demand_level=demand,
                headway_pred=hw_pred,
                headway_status=hw_status,
                source="ml_model",
            )
        except Exception as e:
            # Fallback ke heuristic jika model error
            pass

    # ── FALLBACK HEURISTIC (model belum ditraining) ────
    is_peak  = 1 if hour in list(range(6, 10)) + list(range(16, 20)) else 0
    n_total  = req.n_total or (8 if feeder else 12)
    pct_eff  = 0.85 if is_peak else 0.70
    ne_est   = max(1, int(n_total * pct_eff))
    cycle    = 60 if feeder else 120
    hw_est   = round(cycle / ne_est, 1)

    base_pax = 48 if is_peak else 24
    pax_pred = base_pax
    if req.hujan:
        pax_pred = int(pax_pred * 0.85)

    demand_label = (
        "tinggi" if pax_pred >= 45
        else "sedang" if pax_pred >= 25
        else "rendah"
    )
    armada_rek = max(1, math.ceil(pax_pred / capacity))
    status     = get_status(pax_pred, capacity)
    hw_status  = "BAIK" if hw_est <= SPM_HEADWAY else "BURUK"

    return PredictResponse(
        prediksi_penumpang=pax_pred,
        armada_rekomendasi=armada_rek,
        status=status,
        confidence=0.60,
        demand_level=demand_label,
        headway_pred=hw_est,
        headway_status=hw_status,
        source="heuristic_fallback",
    )


@app.get("/model-info")
def model_info():
    """Info model yang sedang aktif dan metrik evaluasi."""
    metrics_path = os.path.join(MODEL_DIR, "xgboost_metrics.json")
    metrics = {}
    if os.path.exists(metrics_path):
        with open(metrics_path) as f:
            metrics = json.load(f)
    return {
        "model_loaded": MODEL_LOADED,
        "classifier_metrics": metrics,
        "feature_columns": feat_meta,
    }