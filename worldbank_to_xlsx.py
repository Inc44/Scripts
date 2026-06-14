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
WDS_PARAMS = {
	"fl": "projectid",
	"rows": "1000",
	"docty_exact": "Procurement Plan",
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
CLOSING_DATE_CUTOFF = datetime(2026, 7, 1)
BASE_DIR = Path.home() / ".cache/scripts"
CACHE_DIR = BASE_DIR / "cache"
OUTPUT_FILE = "projects.csv"


def generate_cache_id() -> str:
	seed = f"{WDS_PARAMS['count_exact']}|{WDS_PARAMS['sectr_exact']}"
	hashed = hashlib.sha256(seed.encode("utf-8")).digest()
	encoded = base64.urlsafe_b64encode(hashed).decode("ascii")
	return encoded[:11]


CACHE_ID = generate_cache_id()
CACHE_ID_DIR = CACHE_DIR / CACHE_ID
CSV_CACHE_DIR = CACHE_ID_DIR / "csv"
JSON_CACHE_DIR = CACHE_ID_DIR / "json"
PDF_CACHE_DIR = CACHE_ID_DIR / "pdf"
XLSX_CACHE_DIR = CACHE_ID_DIR / "xlsx"


def format_cost(us_cost: str) -> str:
	if not us_cost:
		return ""
	cost = float(us_cost)
	eu_cost = f"{cost:,.2f}".replace(",", " ").replace(".", ",")
	return f"$ {eu_cost}"


def extract_team_leader_names(project: dict) -> str:
	teamleadnames = project.get("teamleadname", [])
	return ", ".join(name.strip() for name in teamleadnames if name.strip())


def is_before_cutoff(iso_closing_date: str) -> bool:
	if not iso_closing_date:
		return True
	year, month, day = iso_closing_date.split("-")
	closing_date = datetime(int(year), int(month), int(day))
	return closing_date >= CLOSING_DATE_CUTOFF


async def fetch(session: aiohttp.ClientSession, url: str) -> bytes | None:
	async with session.get(url) as resp:
		if resp.status == 200:
			return await resp.read()
	return None


async def fetch_and_cache_json(
	session: aiohttp.ClientSession, url: str, cache_path: Path
) -> dict:
	if cache_path.exists():
		with open(cache_path, "r", encoding="utf-8") as file:
			return json.load(file)
	data = await fetch(session, url)
	if data is None:
		return {}
	obj = json.loads(data.decode("utf-8"))
	with open(cache_path, "w", encoding="utf-8") as file:
		json.dump(obj, file, ensure_ascii=False, indent="\t")
	return obj


async def extract_project_ids(
	session: aiohttp.ClientSession, offset: int
) -> tuple[set[str], int]:
	cache_path = JSON_CACHE_DIR / f"wds_{offset}.json"
	params = WDS_PARAMS.copy()
	params["os"] = str(offset)
	url = f"{WDS_URL}?{urlencode(params)}"
	obj = await fetch_and_cache_json(session, url, cache_path)
	project_ids = set()
	total = obj.get("total", 0)
	for key, value in obj.get("documents", {}).items():
		if key == "facets":
			continue
		project_id = value.get("projectid")
		project_ids.add(project_id)
	return project_ids, total


async def extract_all_project_ids(session: aiohttp.ClientSession) -> list[str]:
	project_ids, total = await extract_project_ids(session, 0)
	all_project_ids = set(project_ids)
	offsets = range(1000, total, 1000)
	tasks = [extract_project_ids(session, offset) for offset in offsets]
	pbar = tqdm(total=len(tasks), desc="Pages")
	for task in asyncio.as_completed(tasks):
		project_ids, _ = await task
		all_project_ids.update(project_ids)
		pbar.update()
	pbar.close()
	return sorted(all_project_ids)


def parse_project(project_id: str, obj: dict) -> dict | None:
	project = obj.get("projects", {}).get(project_id)
	if not project:
		return None
	return {
		"Country": project.get("countryshortname", ""),
		"Project ID": project_id,
		"Project Title": project.get("project_name", ""),
		"Team Leader": extract_team_leader_names(project),
		"Effective Date": project.get("loan_effective_date", ""),
		"Closing Date": project.get("closingdate", ""),
		"Total Project Cost": format_cost(project.get("lendprojectcost", "")),
		"Implementing Agency": project.get("impagency", ""),
	}


async def extract_project(
	session: aiohttp.ClientSession, project_id: str
) -> tuple[str, dict | None]:
	cache_path = JSON_CACHE_DIR / f"projects_{project_id}.json"
	params = {"fl": "*", "id": project_id}
	url = f"{PROJECTS_URL}?{urlencode(params)}"
	obj = await fetch_and_cache_json(session, url, cache_path)
	return project_id, parse_project(project_id, obj)


async def extract_projects(
	session: aiohttp.ClientSession, project_ids: list[str]
) -> dict[str, dict]:
	projects = {}
	tasks = [extract_project(session, project_id) for project_id in project_ids]
	pbar = tqdm(total=len(tasks), desc="Projects")
	for task in asyncio.as_completed(tasks):
		project_id, project = await task
		if project:
			projects[project_id] = project
		pbar.update()
	pbar.close()
	return projects


async def extract_procurement_plan_link(
	session: aiohttp.ClientSession, project_id: str
) -> tuple[str, str]:
	cache_path = JSON_CACHE_DIR / f"wds_{project_id}.json"
	params = {
		"fl": "pdfurl",
		"rows": "1",
		"docty_exact": "Procurement Plan",
		"proid": project_id,
	}
	url = f"{WDS_URL}?{urlencode(params)}"
	obj = await fetch_and_cache_json(session, url, cache_path)
	pdf_url = ""
	for key, value in obj.get("documents", {}).items():
		if key == "facets":
			continue
		pdf_url = value.get("pdfurl", "")
	return project_id, pdf_url


async def extract_procurement_plan_links(
	session: aiohttp.ClientSession, project_ids: list[str]
) -> dict[str, str]:
	pdf_urls = {}
	tasks = [
		extract_procurement_plan_link(session, project_id) for project_id in project_ids
	]
	pbar = tqdm(total=len(tasks), desc="Plans")
	for task in asyncio.as_completed(tasks):
		project_id, pdf_url = await task
		if pdf_url:
			pdf_urls[project_id] = pdf_url
		pbar.update()
	pbar.close()
	return pdf_urls


def build_project_rows(
	projects: dict[str, dict], pdf_urls: dict[str, str]
) -> list[dict]:
	rows = []
	for project_id in sorted(projects.keys()):
		project = projects[project_id]
		pdf_url = ""
		if is_before_cutoff(project.get("Closing Date", "")):
			pdf_url = pdf_urls.get(project_id, "")
		project["Procurement Plan Link"] = pdf_url
		rows.append(project)
	rows.sort(key=lambda row: (row["Country"], row["Project ID"]))
	return rows


def write_rows(rows: list[dict]) -> None:
	with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
		writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
		writer.writeheader()
		writer.writerows(rows)


async def download_pdf(session: aiohttp.ClientSession, pdf_url: str) -> str | None:
	filename = pdf_url.split("/")[-1]
	cache_path = PDF_CACHE_DIR / filename
	if cache_path.exists():
		return cache_path.name
	content = await fetch(session, pdf_url)
	with open(cache_path, "wb") as file:
		file.write(content)
	return cache_path.name


async def download_pdfs(session: aiohttp.ClientSession, rows: list[dict]) -> None:
	tasks = []
	for row in rows:
		pdf_url = row.get("Procurement Plan Link", "")
		if pdf_url:
			tasks.append(download_pdf(session, pdf_url))
	if not tasks:
		print("No PDFs to download")
		return
	pbar = tqdm(total=len(tasks), desc="PDFs")
	for task in asyncio.as_completed(tasks):
		await task
		pbar.update()
	pbar.close()


def cache_init():
	CACHE_ID_DIR.mkdir(parents=True, exist_ok=True)
	CSV_CACHE_DIR.mkdir(parents=True, exist_ok=True)
	JSON_CACHE_DIR.mkdir(parents=True, exist_ok=True)
	PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
	XLSX_CACHE_DIR.mkdir(parents=True, exist_ok=True)


async def main() -> None:
	cache_init()
	async with aiohttp.ClientSession() as session:
		project_ids = await extract_all_project_ids(session)
		projects = await extract_projects(session, project_ids)
		pdf_urls = await extract_procurement_plan_links(session, project_ids)
		rows = build_project_rows(projects, pdf_urls)
		write_rows(rows)
		await download_pdfs(session, rows)


if __name__ == "__main__":
	asyncio.run(main())
