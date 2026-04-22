from __future__ import annotations
from urllib.parse import urlparse, urlencode, unquote
import aiohttp
import asyncio
import json

from tqdm import tqdm

INPUT_FILE = "result.json"
OUTPUT_FILE = "Youtube.txt"
CONCURRENCY = 20
OEMBED_URL = "https://www.youtube.com/oembed"


def extract_youtube_urls(data: dict) -> set[str]:
	urls = set()
	for message in data.get("messages", []):
		for entity in message.get("text_entities", []):
			if entity.get("type") == "link":
				text = entity.get("text", "")
				if text.startswith("http") and "youtu" in text.lower():
					urls.add(text)
	return urls


def normalize_channel_url(url: str) -> str | None:
	parsed = urlparse(url)
	host = parsed.netloc.lstrip("m.")
	if "youtube.com" not in host:
		return None
	parts = [segment for segment in parsed.path.split("/") if segment]
	if not parts:
		return None
	first = parts[0]
	if first.startswith("@"):
		return f"https://www.youtube.com/{first}"
	if first in ("channel", "c", "user") and len(parts) >= 2:
		return f"https://www.youtube.com/{first}/{parts[1]}"
	return None


def is_resolvable(url: str) -> bool:
	parsed = urlparse(url)
	host = parsed.netloc
	parts = [segment for segment in parsed.path.split("/") if segment]
	if host == "youtu.be":
		return len(parts) > 0
	if "youtube.com" in host:  # and "music.youtube.com" not in host:
		if parts and parts[0] in ("watch", "shorts", "live", "playlist"):
			return True
	return False


async def resolve_channel_url(session: aiohttp.ClientSession, url: str) -> str | None:
	parameters = urlencode({"url": url, "format": "json"})
	endpoint = f"{OEMBED_URL}?{parameters}"
	response = await session.get(endpoint, timeout=aiohttp.ClientTimeout(total=30))
	if response.status == 200:
		data = await response.json()
		return data.get("author_url")
	return None


def separate_urls(urls: set[str]) -> tuple[set[str], list[str]]:
	channel_urls = set()
	urls_to_resolve = []
	for url in urls:
		channel_url = normalize_channel_url(url)
		if channel_url:
			channel_urls.add(channel_url)
		elif is_resolvable(url):
			urls_to_resolve.append(url)
	return channel_urls, urls_to_resolve


async def resolve_channel_urls(
	session: aiohttp.ClientSession,
	urls_to_resolve: list[str],
	channel_urls: set[str],
) -> None:
	tasks = [resolve_channel_url(session, url) for url in urls_to_resolve]
	for task in tqdm(
		asyncio.as_completed(tasks), total=len(tasks), desc="Resolving channels"
	):
		result = await task
		if result:
			channel_urls.add(result)


def normalize_final_channel_url(url: str) -> str:
	if url.startswith("/"):
		url = "https://www.youtube.com" + url
	return unquote(url)


async def main() -> None:
	with open(INPUT_FILE, encoding="utf-8") as file:
		data = json.load(file)
	urls = extract_youtube_urls(data)
	print(f"Found {len(urls)} unique YouTube URLs:")
	channel_urls, urls_to_resolve = separate_urls(urls)
	print(f"- {len(channel_urls)} channel URLs")
	print(f"- {len(urls_to_resolve)} URLs to resolve via oEmbed")
	async with aiohttp.ClientSession() as session:
		await resolve_channel_urls(session, urls_to_resolve, channel_urls)
	normalized_channel_urls = {
		normalize_final_channel_url(channel) for channel in channel_urls
	}
	sorted_channels = sorted(normalized_channel_urls, key=str.lower)
	with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
		for channel in sorted_channels:
			file.write(channel + "\n")
	print(f"- {len(sorted_channels)} unique channel URLs written to {OUTPUT_FILE}")


if __name__ == "__main__":
	asyncio.run(main())
