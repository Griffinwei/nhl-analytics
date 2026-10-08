"""Score shots with the champion xG model and upsert them into xg_predictions.

    python -m models.xg.score

Only shots without a prediction from the current champion are scored, so this is cheap to run
nightly and rescores everything after a promotion:
    MlflowClient().set_registered_model_alias('xg', 'champion', <version>)
"""
import mlflow
import pandas as pd
import sqlalchemy as sa
from mlflow import MlflowClient

from ingestion.common import ensure_schema, get_engine
from models.xg.features import add_features

MODEL_NAME = 'xg'

UNSCORED_SQL = """
SELECT f.*
FROM transformations.int_shot_features f
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


def load_champion():
    mlflow.set_tracking_uri('sqlite:///mlflow.db')
    # Registry version number, stored as text in xg_predictions.model_version
    version = str(MlflowClient().get_model_version_by_alias(MODEL_NAME, 'champion').version)
    # Load by number, not alias, so the model always matches the version written alongside it
    return mlflow.sklearn.load_model(f'models:/{MODEL_NAME}/{version}'), version


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
    print(f"Scored {len(predictions):,} shots with {MODEL_NAME} v{version}")
