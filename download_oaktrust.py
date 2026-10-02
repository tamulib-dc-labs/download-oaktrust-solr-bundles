from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

import httpx
from tqdm import tqdm

DEFAULT_API = "https://oaktrust.library.tamu.edu/server/api"
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "oaktrust-bundle-downloader/1.0 (python-httpx)",
}


def load_item_ids(path: Path) -> list[str]:
    data = json.loads(path.read_text())
    docs = data["response"]["docs"]
    return [doc["search.resourceid"] for doc in docs]


def get_json(client: httpx.Client, url: str, params: dict | None = None) -> dict:
    resp = client.get(url, params=params, headers=HEADERS)
    if resp.headers.get("cf-mitigated") == "challenge":
        sys.exit(
            f"Blocked by Cloudflare challenge at {url}.\n"
            "The OakTrust REST API is not reachable from this network without a browser; "
            "try from the TAMU network/VPN or ask the OakTrust admins to allowlist your IP."
        )
    resp.raise_for_status()
    return resp.json()


def paginate(client: httpx.Client, url: str, key: str) -> list[dict]:
    """Follow DSpace HAL pagination and collect all embedded objects under `key`."""
    results: list[dict] = []
    next_url: str | None = url
    params: dict | None = {"size": 100}
    while next_url:
        page = get_json(client, next_url, params)
        results.extend(page.get("_embedded", {}).get(key, []))
        next_url = page.get("_links", {}).get("next", {}).get("href")
        params = None
    return results


def safe_name(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", name).strip() or "unnamed"


def md5_of(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download_bitstream(client: httpx.Client, api: str, bs: dict, dest: Path) -> str:
    checksum = (bs.get("checkSum") or {}).get("value")
    algo = ((bs.get("checkSum") or {}).get("checkSumAlgorithm") or "").upper()
    verify = checksum if algo == "MD5" else None

    if dest.exists() and (
        (verify and md5_of(dest) == verify)
        or (not verify and dest.stat().st_size == bs.get("sizeBytes"))
    ):
        return "skipped"

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    url = f"{api}/core/bitstreams/{bs['uuid']}/content"
    with client.stream("GET", url, headers={"User-Agent": HEADERS["User-Agent"]}) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in resp.iter_bytes(1 << 16):
                f.write(chunk)

    if verify and md5_of(tmp) != verify:
        tmp.unlink()
        raise ValueError(f"checksum mismatch for {dest}")
    tmp.replace(dest)
    return "downloaded"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-i", "--input", type=Path, default=Path("pdfs.json"), help="Solr JSON with search.resourceid docs")
    parser.add_argument("-o", "--output", type=Path, default=Path("downloads"), help="output directory")
    parser.add_argument("--api", default=DEFAULT_API, help="DSpace REST API base URL")
    parser.add_argument("--bundles", nargs="*", help="only these bundle names (default: all bundles)")
    args = parser.parse_args()

    api = args.api.rstrip("/")
    item_ids = load_item_ids(args.input)
    wanted = {b.upper() for b in args.bundles} if args.bundles else None
    counts = {"downloaded": 0, "skipped": 0, "failed": 0}

    with httpx.Client(timeout=httpx.Timeout(60.0, read=300.0), follow_redirects=True) as client:
        for item_id in tqdm(item_ids, desc="items", unit="item"):
            try:
                bundles = paginate(client, f"{api}/core/items/{item_id}/bundles", "bundles")
            except httpx.HTTPError as e:
                tqdm.write(f"[{item_id}] could not list bundles: {e}")
                counts["failed"] += 1
                continue

            for bundle in bundles:
                if wanted and bundle["name"].upper() not in wanted:
                    continue
                bitstreams = paginate(client, bundle["_links"]["bitstreams"]["href"], "bitstreams")
                for bs in bitstreams:
                    dest = args.output / item_id / safe_name(bundle["name"]) / safe_name(bs.get("name") or bs["uuid"])
                    try:
                        counts[download_bitstream(client, api, bs, dest)] += 1
                    except (httpx.HTTPError, ValueError) as e:
                        tqdm.write(f"[{item_id}] {bundle['name']}/{bs.get('name')}: {e}")
                        counts["failed"] += 1

    print(f"Done: {counts['downloaded']} downloaded, {counts['skipped']} skipped, {counts['failed']} failed -> {args.output}")


if __name__ == "__main__":
    main()
