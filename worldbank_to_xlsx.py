from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import aiohttp
from tqdm import tqdm

WDS_URL = "https://search.worldbank.org/api/v3/wds"
PROJECTS_URL = "https://search.worldbank.org/api/v3/projects"
WDS_PARAMETERS = {
	"fl": "projectid",
	"docty": "Procurement Plan",
	"rows": "1000",
	"count_exact": "",
	"sectr_exact": "",
}
FIELDNAMES = [
	"Country",
	"Project ID",
	"Project Title",
	"Team Leader",
	"Effective Date",
	"Closing Date",
	"Total Project Cost",
	"Implementing Agency",
	"Procurement Plan Link",
	"Activity Reference No. / Description",
	"Method",
	"Market Approach",
	"Estimated Amount (US$)",
	"Process Status",
	"Expression of Interest Notice",
]
BASE_DIR = Path.home() / ".cache/scripts"
CACHE_DIR = BASE_DIR / "cache"
OUTPUT_FILE = "projects.csv"


def generate_cache_id() -> str:
	seed = f"{WDS_PARAMETERS['count_exact']}|{WDS_PARAMETERS['sectr_exact']}"
	hashed = hashlib.sha256(seed.encode("utf-8")).digest()
	encoded = base64.urlsafe_b64encode(hashed).decode("ascii")
	return encoded[:11]


CACHE_ID = generate_cache_id()
CACHE_ID_DIR = CACHE_DIR / CACHE_ID
JSON_CACHE_DIR = CACHE_ID_DIR / "json"
PDF_CACHE_DIR = CACHE_ID_DIR / "pdf"


def format_cost(us_cost: str) -> str:
	if not us_cost:
		return ""
	cost = float(us_cost)
	eu_cost = f"{cost:,.2f}".replace(",", " ").replace(".", ",")
	return f"$ {eu_cost}"


def extract_team_leader_names(project: dict) -> str:
	teamleadnames = project.get("teamleadname", [])
	return ", ".join(name.strip() for name in teamleadnames if name.strip())
