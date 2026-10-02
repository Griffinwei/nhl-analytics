import math
import os
from typing import Optional
from datetime import datetime
import requests
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

# Ingests edge stats data using the NHL API and stores it in Neon Postgres

# Schema:
# - player_id: int
# - game_id: int
# - season: int
# - top_speed: float
# - avg_speed: float
# - distance_skated: float
# - burst_count: int
# - offensive_zone_time_pct: float
# - defensive_zone_time_pct: float
# - neutral_zone_time_pct: float

# TODO: Create ingestion script and table