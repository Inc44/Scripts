from __future__ import annotations

import unicodedata
from datetime import datetime, timezone
from pathlib import Path

INPUT_FILES = [Path("ADECal.ics"), Path("ADECal.vcs")]


def remove_accents(text: str) -> str:
	return unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("ASCII")


def unfold_lines(lines: list[str]) -> list[str]:
	unfolded = []
	for line in lines:
		if line and line[0] in (" ", "\t") and unfolded:
			unfolded[-1] += line[1:]
		else:
			unfolded.append(line)
	return unfolded


def extract_events(unfolded_lines: list[str]) -> list[dict[str, str]]:
	events = []
	event = None
	for line in unfolded_lines:
		if line == "BEGIN:VEVENT":
			event = {}
		elif line == "END:VEVENT":
			if event is not None:
				events.append(event)
			event = None
		elif event is not None and ":" in line:
			key, value = line.split(":", 1)
			key = key.split(";")[0]
			if key not in event:
				event[key] = value
	return events


def format_date(date: str) -> str:
	date = date.rstrip("Z")
	utc_date = datetime.strptime(date, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
	local_date = utc_date.astimezone()
	return local_date.strftime("%Y%m%d_%H%M%S")


def read_calendar() -> list[str] | None:
	for filename in INPUT_FILES:
		if filename.exists():
			with open(filename, "r", encoding="utf-8") as file:
				return file.read().splitlines()
	return None


def search_events(events: list[dict[str, str]], query: str) -> tuple[int, list[str]]:
	count = 0
	matches = []
	normalized_query = remove_accents(query).lower()
	for event in events:
		summary = event.get("SUMMARY", "")
		normalized_summary = remove_accents(summary).lower()
		if normalized_query in normalized_summary:
			count += 1
			date = event.get("DTSTART", "")
			if "T" in date:
				formatted_date = format_date(date)
				matches.append(f"{formatted_date}: {summary}")
	return count, matches


def main() -> None:
	lines = read_calendar()
	if lines is None:
		print("Count: 0")
		return
	events = extract_events(unfold_lines(lines))
	query = input("Enter your query: ").strip()
	count, matches = search_events(events, query)
	matches.sort()
	print(f"Count: {count}")
	for line in matches:
		print(line)


if __name__ == "__main__":
	main()
