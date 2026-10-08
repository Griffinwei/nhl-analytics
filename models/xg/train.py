"""Train xG models and log each run to MLflow.

    python -m models.xg.train              # all experiments
    python -m models.xg.train 1-location   # just the named ones
    mlflow ui --backend-store-uri sqlite:///mlflow.db
"""
import sys

import mlflow
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import SplineTransformer

from models.xg.features import load_shots

TRAIN, VAL, TEST = 20242025, 20252026, 20262027

EXPERIMENTS = {
    '1-location': {'spline': ['distance', 'angle']},
}


def build_model(features: dict):
    return make_pipeline(
        ColumnTransformer([('spline', SplineTransformer(n_knots=8, knots='quantile', extrapolation='constant'), features['spline'])]),
        LogisticRegression(max_iter=1000),
    )


def evaluate(y, p, base_rate) -> dict:
    return {
        'log_loss': log_loss(y, p),
        'base_rate_log_loss': log_loss(y, np.full(len(y), base_rate)),
        'auc': roc_auc_score(y, p),
        'brier': brier_score_loss(y, p),
        'xg_over_goals': p.sum() / y.sum(),
    }


def run(name: str, features: dict, shots):
    cols = [c for group in features.values() for c in group]
    split = {k: shots[shots.season == s] for k, s in (('train', TRAIN), ('val', VAL), ('test', TEST))}
    model = build_model(features).fit(split['train'][cols], split['train'].is_goal)
    base_rate = split['train'].is_goal.mean()

    with mlflow.start_run(run_name=name):
        mlflow.log_params({'features': cols, 'train': TRAIN, 'val': VAL, 'test': TEST})
        for k, df in split.items():
            metrics = evaluate(df.is_goal, model.predict_proba(df[cols])[:, 1], base_rate)
            mlflow.log_metrics({f'{k}_{m}': v for m, v in metrics.items()})
            print(f"{name:24s} {k:5s} " + '  '.join(f'{m} {v:.5f}' for m, v in metrics.items()))
        mlflow.sklearn.log_model(model, name='model', serialization_format='cloudpickle')


if __name__ == '__main__':
    mlflow.set_tracking_uri('sqlite:///mlflow.db')
    mlflow.set_experiment('xg')
    shots = load_shots()
    for name in sys.argv[1:] or EXPERIMENTS:
        run(name, EXPERIMENTS[name], shots)
