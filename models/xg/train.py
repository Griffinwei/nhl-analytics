"""Train xG models and log each run to MLflow.

    python -m models.xg.train              # all experiments
    python -m models.xg.train 1-location   # just the named ones
    mlflow ui --backend-store-uri sqlite:///mlflow.db
"""
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mlflow
import numpy as np
from sklearn.calibration import CalibrationDisplay
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, SplineTransformer, StandardScaler
from xgboost import XGBClassifier

from models.xg.features import load_shots

TRAIN, VAL, TEST = 20242025, 20252026, 20262027
COLORS = {'val': '#2a78d6', 'test': '#eb6834'}

LOCATION = {'spline': ['distance', 'angle']}
GAME_STATE = {
    **LOCATION,
    'numeric': ['empty_net', 'empty_net_distance', 'shooter_skaters', 'defender_skaters'],
    'categorical': ['shot_type', 'strength'],
}

EXPERIMENTS = {
    '1-location': ('logistic', LOCATION),
    '2-game-state': ('logistic', GAME_STATE),
    '4-xgboost': ('xgboost', GAME_STATE),
}


def build_model(kind: str, features: dict):
    transformers = {
        # Trees find their own non-linearities, so XGBoost gets the raw values
        'spline': SplineTransformer(n_knots=8, knots='quantile', extrapolation='constant') if kind == 'logistic' else 'passthrough',
        'numeric': StandardScaler(),
        # Shot types seen fewer than 100 times in training (between-legs, cradle, new ones) share one column
        'categorical': OneHotEncoder(handle_unknown='infrequent_if_exist', min_frequency=100),
    }
    return make_pipeline(
        ColumnTransformer([(group, transformers[group], cols) for group, cols in features.items()]),
        LogisticRegression(max_iter=1000) if kind == 'logistic' else
        XGBClassifier(n_estimators=2000, learning_rate=0.05, max_depth=4, early_stopping_rounds=50, eval_metric='logloss'),
    )


def evaluate(y, p, base_rate) -> dict:
    return {
        'log_loss': log_loss(y, p),
        'base_rate_log_loss': log_loss(y, np.full(len(y), base_rate)),
        'auc': roc_auc_score(y, p),
        'brier': brier_score_loss(y, p),
        'xg_over_goals': p.sum() / y.sum(),
    }


def run(name: str, kind: str, features: dict, shots):
    cols = [c for group in features.values() for c in group]
    split = {k: shots[shots.season == s] for k, s in (('train', TRAIN), ('val', VAL), ('test', TEST))}
    model = build_model(kind, features)
    if kind == 'xgboost':
        # Early stopping needs the validation season already run through the fitted preprocessing step
        prep, xgb = model[0], model[-1]
        X_train = prep.fit_transform(split['train'][cols])
        xgb.fit(X_train, split['train'].is_goal, eval_set=[(prep.transform(split['val'][cols]), split['val'].is_goal)], verbose=False)
    else:
        model.fit(split['train'][cols], split['train'].is_goal)
    base_rate = split['train'].is_goal.mean()

    fig, ax = plt.subplots(figsize=(6, 6))
    with mlflow.start_run(run_name=name):
        mlflow.log_params({'model': kind, 'features': cols, 'train': TRAIN, 'val': VAL, 'test': TEST})
        for k, df in split.items():
            p = model.predict_proba(df[cols])[:, 1]
            metrics = evaluate(df.is_goal, p, base_rate)
            mlflow.log_metrics({f'{k}_{m}': v for m, v in metrics.items()})
            print(f"{name:24s} {k:5s} " + '  '.join(f'{m} {v:.5f}' for m, v in metrics.items()))
            if k != 'train':
                CalibrationDisplay.from_predictions(df.is_goal, p, n_bins=20, strategy='quantile', name=f'{k} {df.season.iloc[0]}', ax=ax,
                                                    color=COLORS[k])
        ax.set(xlim=(0, 0.5), ylim=(0, 0.5), xlabel='Mean predicted xG (20 quantile bins)', ylabel='Observed goal rate', title=name)
        mlflow.log_figure(fig, 'calibration.png')
        plt.close(fig)
        mlflow.sklearn.log_model(model, name='model', serialization_format='cloudpickle')


if __name__ == '__main__':
    mlflow.set_tracking_uri('sqlite:///mlflow.db')
    mlflow.set_experiment('xg')
    shots = load_shots()
    for name in sys.argv[1:] or EXPERIMENTS:
        run(name, *EXPERIMENTS[name], shots)
