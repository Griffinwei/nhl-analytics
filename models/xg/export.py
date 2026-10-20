"""Export the registry's champion xG model to models/xg/production/ for GitHub Actions.

    python -m models.xg.export

Run after promoting a new champion and commit the folder. The nightly workflow scores with
model.pkl and builds the marts for the version in metadata.json.
"""
import json
import pickle

import sklearn
import xgboost

from models.xg.score import MODEL_NAME, PRODUCTION, load_from_registry

if __name__ == '__main__':
    model, version = load_from_registry()
    PRODUCTION.mkdir(exist_ok=True)
    # Plain pickle, so loading needs only the libraries the pipeline is built from, not MLflow.
    # Pickles are tied to library versions: CI installs the same pins from pyproject.toml's score group
    with open(PRODUCTION / 'model.pkl', 'wb') as f:
        pickle.dump(model, f)
    (PRODUCTION / 'metadata.json').write_text(json.dumps({
        'name': MODEL_NAME,
        'version': version,
        'features': list(model.feature_names_in_),
        'scikit-learn': sklearn.__version__,
        'xgboost': xgboost.__version__,
    }, indent=2) + '\n')
    print(f"Exported {MODEL_NAME} v{version} to {PRODUCTION}")
