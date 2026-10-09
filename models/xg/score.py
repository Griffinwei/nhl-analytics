"""Score shots with the champion xG model and upsert them into xg_predictions.

    python -m models.xg.score

Only shots without a prediction from the current champion are scored, so this is cheap to run
nightly and rescores everything after a promotion:
    MlflowClient().set_registered_model_alias('xg', 'champion', <version>)
    python -m models.xg.export   # then commit models/xg/production/ so GitHub Actions picks it up

The champion comes from the local MLflow registry (mlflow.db) when there is one, otherwise from the
copy exported to models/xg/production/ (GitHub Actions has no registry).
"""
import json
import os
import pickle
from pathlib import Path

import mlflow
import pandas as pd
import sqlalchemy as sa
from mlflow import MlflowClient

from ingestion.common import ensure_schema, get_engine
from models.xg.features import add_features

MODEL_NAME = 'xg'
REGISTRY = Path('mlflow.db')
PRODUCTION = Path(__file__).parent / 'production'
# Schema dbt builds int_shot_features into: the dev target's by default, the prod target's in GitHub Actions
FEATURES_SCHEMA = os.getenv('DBT_SCHEMA', 'transformations')

UNSCORED_SQL = f"""
SELECT f.*
FROM {FEATURES_SCHEMA}.int_shot_features f
WHERE NOT EXISTS (
    SELECT 1 FROM xg_predictions p
    WHERE p.game_id = f.game_id AND p.event_id = f.event_id AND p.model_version = :version
)
"""

UPSERT_SQL = """
INSERT INTO xg_predictions (game_id, event_id, model_version, xg)
VALUES (:game_id, :event_id, :model_version, :xg)
ON CONFLICT (game_id, event_id, model_version) DO UPDATE SET
    xg = EXCLUDED.xg,
    scored_at = now()
"""


def load_from_registry():
    mlflow.set_tracking_uri(f'sqlite:///{REGISTRY}')
    # Registry version number, stored as text in xg_predictions.model_version
    version = str(MlflowClient().get_model_version_by_alias(MODEL_NAME, 'champion').version)
    # Load by number, not alias, so the model always matches the version written alongside it
    return mlflow.sklearn.load_model(f'models:/{MODEL_NAME}/{version}'), version


def load_from_production():
    meta = json.loads((PRODUCTION / 'metadata.json').read_text())
    with open(PRODUCTION / 'model.pkl', 'rb') as f:
        model = pickle.load(f)
    if list(model.feature_names_in_) != meta['features']:
        raise ValueError(f"{PRODUCTION / 'model.pkl'} does not match the feature list in metadata.json; re-run models.xg.export")
    return model, meta['version']


def load_champion():
    return load_from_registry() if REGISTRY.exists() else load_from_production()


def score(engine: sa.Engine, model, version: str) -> pd.DataFrame:
    with engine.connect() as conn:
        shots = pd.read_sql(sa.text(UNSCORED_SQL), conn, params={'version': version})
    shots = add_features(shots)
    xg = model.predict_proba(shots[model.feature_names_in_])[:, 1]
    return shots[['game_id', 'event_id']].assign(model_version=version, xg=xg.astype(float))


if __name__ == '__main__':
    engine = get_engine()
    ensure_schema(engine)
    model, version = load_champion()
    predictions = score(engine, model, version)
    if len(predictions):
        with engine.begin() as conn:
            conn.execute(sa.text(UPSERT_SQL), predictions.to_dict('records'))
    source = REGISTRY if REGISTRY.exists() else PRODUCTION
    print(f"Scored {len(predictions):,} shots from {FEATURES_SCHEMA}.int_shot_features with {MODEL_NAME} v{version} ({source})")
