"""
P3 — Training XGBoost Regressor: Prediksi Headway & Jumlah Bus Efektif
=======================================================================
Model ini memprediksi nilai numerik (headway_real_min, n_efektif)
yang dipakai FastAPI untuk menghitung armada rekomendasi.

Input  : feature_engineered.csv (output P5)
Output : models/xgb_regressor_headway.pkl
         models/xgb_regressor_nefektif.pkl
"""

import pandas as pd
import numpy as np
import json, csv, pickle, os, warnings
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb
import optuna

warnings.filterwarnings("ignore")
optuna.logging.set_verbosity(optuna.logging.WARNING)

FEATURE_FILE  = "../feature_engineered.csv"
MODEL_DIR     = "models"
RANDOM_STATE  = 42
N_TRIALS      = 20

os.makedirs(MODEL_DIR, exist_ok=True)

print("=" * 60)
print("P3 — XGBoost Regressor: Headway & Bus Efektif")
print("=" * 60)

if not os.path.exists(FEATURE_FILE):
    raise FileNotFoundError(f"File tidak ditemukan: {FEATURE_FILE}")

df = pd.read_csv(FEATURE_FILE, quoting=csv.QUOTE_ALL, on_bad_lines="skip", engine="python")
print(f"Data: {df.shape[0]} baris")

# Encode kategorik
le_time = LabelEncoder()
le_svc  = LabelEncoder()
df["time_category_enc"] = le_time.fit_transform(df["time_category"].astype(str))
df["service_level_enc"] = le_svc.fit_transform(df["service_level"].astype(str))

FEATURES = [
    "hour", "is_peak_enc", "is_weekend_enc", "feeder_enc",
    "n_total", "pct_efektif", "avg_speed_kmh", "pct_mangkal",
    "time_category_enc", "service_level_enc",
]

def train_regressor(target_col, label):
    df_clean = df.dropna(subset=FEATURES + [target_col])
    X = df_clean[FEATURES].astype(float)
    y = df_clean[target_col].astype(float)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_STATE
    )

    def objective(trial):
        params = {
            "n_estimators":     trial.suggest_int("n_estimators", 100, 400),
            "max_depth":        trial.suggest_int("max_depth", 3, 7),
            "learning_rate":    trial.suggest_float("lr", 0.01, 0.3, log=True),
            "subsample":        trial.suggest_float("sub", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("col", 0.6, 1.0),
            "objective":        "reg:squarederror",
            "random_state":     RANDOM_STATE,
            "n_jobs":           -1,
        }
        m = xgb.XGBRegressor(**params)
        m.fit(X_train, y_train, verbose=False)
        pred = m.predict(X_test)
        return -mean_absolute_error(y_test, pred)

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=N_TRIALS)

    best = study.best_params
    best.update({"objective": "reg:squarederror", "random_state": RANDOM_STATE, "n_jobs": -1})
    model = xgb.XGBRegressor(**best)
    model.fit(X_train, y_train, verbose=False)

    pred = model.predict(X_test)
    mae  = mean_absolute_error(y_test, pred)
    rmse = np.sqrt(mean_squared_error(y_test, pred))
    mape = np.mean(np.abs((y_test - pred) / (y_test + 1e-9))) * 100

    print(f"\n  [{label}] MAE={mae:.2f} | RMSE={rmse:.2f} | MAPE={mape:.1f}%")
    return model

print("\n[1] Training regressor: headway_real_min ...")
model_hw = train_regressor("headway_real_min", "headway")

print("\n[2] Training regressor: n_efektif ...")
model_ne = train_regressor("n_efektif", "n_efektif")

with open(f"{MODEL_DIR}/xgb_regressor_headway.pkl", "wb") as f:
    pickle.dump(model_hw, f)
with open(f"{MODEL_DIR}/xgb_regressor_nefektif.pkl", "wb") as f:
    pickle.dump(model_ne, f)

# Simpan feature columns untuk inference
with open(f"{MODEL_DIR}/regressor_feature_columns.json", "w") as f:
    json.dump({
        "features": FEATURES,
        "time_category_classes": le_time.classes_.tolist(),
        "service_level_classes": le_svc.classes_.tolist(),
    }, f, indent=2)

with open(f"{MODEL_DIR}/label_encoders_regressor.pkl", "wb") as f:
    pickle.dump({"time_category": le_time, "service_level": le_svc}, f)

print("\n    ✓ models/xgb_regressor_headway.pkl")
print("    ✓ models/xgb_regressor_nefektif.pkl")
print("    ✓ models/regressor_feature_columns.json")
print("\nTraining regressor selesai!")