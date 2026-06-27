"""
P3 — Training XGBoost: Prediksi Demand Level SuroboyoBus
=========================================================
Input  : feature_engineered.csv (output P5)
Output : models/xgboost_model.pkl, models/label_encoder.pkl,
         models/feature_columns.json
"""

import pandas as pd
import numpy as np
import json
import csv
import pickle
import os

from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import classification_report, confusion_matrix
import xgboost as xgb
import optuna
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────
FEATURE_FILE   = "../feature_engineered.csv"  # output P5
MODEL_DIR      = "models"
RANDOM_STATE   = 42
N_TRIALS_OPTUNA = 30   # turunkan ke 10 untuk test cepat

# Fitur numerik & kategorik yang dipakai
NUMERIC_FEATURES = [
    "hour", "is_peak_enc", "is_weekend_enc", "feeder_enc",
    "n_total", "n_efektif", "pct_efektif",
    "headway_real_min", "headway_gap_vs_spm",
    "avg_speed_kmh", "pct_mangkal",
]
CATEGORICAL_FEATURES = ["time_category", "service_level"]
TARGET = "demand_level"

os.makedirs(MODEL_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────
# 1. LOAD DATA
# ─────────────────────────────────────────────────────────
print("=" * 60)
print("P3 — XGBoost Training: SuroboyoBus Demand Prediction")
print("=" * 60)

if not os.path.exists(FEATURE_FILE):
    raise FileNotFoundError(
        f"File tidak ditemukan: {FEATURE_FILE}\n"
        "Pastikan Notebook_P5.ipynb sudah dijalankan dan menghasilkan feature_engineered.csv"
    )

df = pd.read_csv(FEATURE_FILE, quoting=csv.QUOTE_ALL, on_bad_lines="skip", engine="python")
print(f"\n[1] Data dimuat: {df.shape[0]} baris x {df.shape[1]} kolom")
print(f"    Distribusi demand_level:\n{df[TARGET].value_counts().to_string()}")

# ─────────────────────────────────────────────────────────
# 2. PREPROCESSING
# ─────────────────────────────────────────────────────────
# Drop baris dengan target null
df = df.dropna(subset=[TARGET] + NUMERIC_FEATURES)

# Encode kategorik
le_time    = LabelEncoder()
le_svc     = LabelEncoder()
le_target  = LabelEncoder()

df["time_category_enc"] = le_time.fit_transform(df["time_category"].astype(str))
df["service_level_enc"] = le_svc.fit_transform(df["service_level"].astype(str))
y = le_target.fit_transform(df[TARGET].astype(str))

FINAL_FEATURES = NUMERIC_FEATURES + ["time_category_enc", "service_level_enc"]
X = df[FINAL_FEATURES].astype(float)

# Simpan kolom fitur untuk dipakai di inference
with open(f"{MODEL_DIR}/feature_columns.json", "w") as f:
    json.dump({
        "numeric": NUMERIC_FEATURES,
        "categorical": CATEGORICAL_FEATURES,
        "final": FINAL_FEATURES,
        "target_classes": le_target.classes_.tolist(),
        "time_category_classes": le_time.classes_.tolist(),
        "service_level_classes": le_svc.classes_.tolist(),
    }, f, indent=2)

print(f"\n[2] Preprocessing selesai:")
print(f"    Features : {len(FINAL_FEATURES)} kolom")
print(f"    Target   : {le_target.classes_.tolist()}")
print(f"    Samples  : {len(X)} baris")

# Train/test split — stratified agar distribusi kelas seimbang
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=RANDOM_STATE, stratify=y
)
print(f"    Train: {len(X_train)} | Test: {len(X_test)}")

# ─────────────────────────────────────────────────────────
# 3. HYPERPARAMETER TUNING dengan OPTUNA
# ─────────────────────────────────────────────────────────
print(f"\n[3] Optuna hyperparameter tuning ({N_TRIALS_OPTUNA} trials)...")

def objective(trial):
    params = {
        "n_estimators":     trial.suggest_int("n_estimators", 100, 500),
        "max_depth":        trial.suggest_int("max_depth", 3, 8),
        "learning_rate":    trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "subsample":        trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
        "gamma":            trial.suggest_float("gamma", 0, 5),
        "reg_alpha":        trial.suggest_float("reg_alpha", 0, 1),
        "reg_lambda":       trial.suggest_float("reg_lambda", 0, 1),
        "objective":        "multi:softmax",
        "num_class":        len(le_target.classes_),
        "eval_metric":      "mlogloss",
        "use_label_encoder": False,
        "random_state":     RANDOM_STATE,
        "n_jobs":           -1,
    }
    cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=RANDOM_STATE)
    scores = []
    for train_idx, val_idx in cv.split(X_train, y_train):
        model = xgb.XGBClassifier(**params)
        model.fit(X_train.iloc[train_idx], y_train[train_idx], verbose=False)
        scores.append(model.score(X_train.iloc[val_idx], y_train[val_idx]))
    return np.mean(scores)

optuna.logging.set_verbosity(optuna.logging.WARNING)
study = optuna.create_study(direction="maximize")
study.optimize(objective, n_trials=N_TRIALS_OPTUNA, show_progress_bar=True)

best_params = study.best_params
best_params.update({
    "objective": "multi:softmax",
    "num_class": len(le_target.classes_),
    "eval_metric": "mlogloss",
    "use_label_encoder": False,
    "random_state": RANDOM_STATE,
    "n_jobs": -1,
})
print(f"    Best CV accuracy: {study.best_value:.4f}")
print(f"    Best params: {best_params}")

# ─────────────────────────────────────────────────────────
# 4. TRAIN MODEL FINAL
# ─────────────────────────────────────────────────────────
print("\n[4] Training model final dengan best params...")

model = xgb.XGBClassifier(**best_params)
model.fit(X_train, y_train, verbose=False)

# ─────────────────────────────────────────────────────────
# 5. EVALUASI
# ─────────────────────────────────────────────────────────
y_pred = model.predict(X_test)
y_pred_labels = le_target.inverse_transform(y_pred)
y_test_labels = le_target.inverse_transform(y_test)

print("\n[5] Evaluasi pada Test Set:")
print(classification_report(y_test_labels, y_pred_labels, zero_division=0))

acc = model.score(X_test, y_test)
print(f"    Overall Accuracy: {acc:.4f} ({acc*100:.1f}%)")

# Feature importance
fi = pd.Series(model.feature_importances_, index=FINAL_FEATURES).sort_values(ascending=False)
print("\n    Top 10 Feature Importance:")
print(fi.head(10).to_string())

# ─────────────────────────────────────────────────────────
# 6. SIMPAN MODEL + ENCODER
# ─────────────────────────────────────────────────────────
print("\n[6] Menyimpan model...")

with open(f"{MODEL_DIR}/xgboost_model.pkl", "wb") as f:
    pickle.dump(model, f)

with open(f"{MODEL_DIR}/label_encoder.pkl", "wb") as f:
    pickle.dump({
        "target":        le_target,
        "time_category": le_time,
        "service_level": le_svc,
    }, f)

# Simpan juga metrik evaluasi
metrics = {
    "accuracy":        round(acc, 4),
    "n_train":         len(X_train),
    "n_test":          len(X_test),
    "best_cv_accuracy": round(study.best_value, 4),
    "classes":         le_target.classes_.tolist(),
    "feature_importance": fi.to_dict(),
}
with open(f"{MODEL_DIR}/xgboost_metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)

print(f"    ✓ models/xgboost_model.pkl")
print(f"    ✓ models/label_encoder.pkl")
print(f"    ✓ models/feature_columns.json")
print(f"    ✓ models/xgboost_metrics.json")
print("\nTraining XGBoost selesai!")