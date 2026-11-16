#!/usr/bin/env python3
"""Command-line client for the Mini Distributed Cloud Object Store.

Talks only to the controller's REST API; it never learns or uses worker addresses.
"""
import argparse
import hashlib
import json
import mimetypes
import os
import shutil
import sys

import httpx
from rich.console import Console
from rich.table import Table

BASE_URL = os.environ.get("OBJSTORE_URL", "http://localhost:8000").rstrip("/")
API = f"{BASE_URL}/api/v1"
# Tables carry full chunk IDs, so never render narrower than 125 columns.
console = Console(width=max(125, shutil.get_terminal_size((125, 24)).columns))
errors = Console(stderr=True)

UNITS = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}


# -- helpers -----------------------------------------------------------------
def fail(message: str, code: int = 1):
    errors.print(f"[bold red]error:[/] {message}")
    sys.exit(code)


def check(resp: httpx.Response, expected: int) -> httpx.Response:
    if resp.status_code != expected:
        try:
            err = resp.json()["error"]
            fail(f"HTTP {resp.status_code} {err['code']}: {err['message']}")
        except (ValueError, KeyError, TypeError):
            fail(f"HTTP {resp.status_code}: {resp.text[:300]}")
    return resp


def http() -> httpx.Client:
    return httpx.Client(timeout=httpx.Timeout(300.0, connect=5.0))


def request(method: str, url: str, expected: int, **kwargs) -> httpx.Response:
    try:
        with http() as client:
            return check(client.request(method, url, **kwargs), expected)
    except httpx.HTTPError as exc:
        fail(f"cannot reach the controller at {BASE_URL} ({type(exc).__name__})")


def human(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{int(value)} {unit}" if unit == "B" else f"{value:.2f} {unit}"
        value /= 1024


def parse_size(text: str) -> int:
    raw = text.strip().upper()
    for unit in ("GB", "MB", "KB", "B"):
        if raw.endswith(unit):
            return int(float(raw[: -len(unit)]) * UNITS[unit])
    return int(raw)


def parse_pairs(pairs: list[str]) -> dict:
    result = {}
    for pair in pairs or []:
        if "=" not in pair:
            fail(f"metadata must be given as key=value, got '{pair}'")
        key, value = pair.split("=", 1)
        result[key] = value
    return result


def file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def print_json(data) -> None:
    print(json.dumps(data, indent=2))


STATUS_STYLE = {
    "healthy": "green", "under_replicated": "yellow", "unavailable": "bold red", "over_replicated": "cyan",
    "ALIVE": "green", "DEAD": "bold red", "HEALTHY": "green", "CORRUPT": "bold red",
}


def styled(text: str) -> str:
    style = STATUS_STYLE.get(text)
    return f"[{style}]{text}[/]" if style else text


def show_object(doc: dict) -> None:
    info = Table(show_header=False, box=None, padding=(0, 2))
    info.add_row("Object ID", doc["object_id"])
    info.add_row("Name", doc["name"])
    info.add_row("Size", f"{doc['size']} bytes ({human(doc['size'])})")
    info.add_row("Content type", doc["content_type"])
    info.add_row("Checksum (SHA-256)", doc["checksum"])
    info.add_row("Chunks", f"{doc['chunk_count']} (chunk size {doc['chunk_size']} bytes)")
    info.add_row("Created", doc["created_at"])
    info.add_row("Last modified", doc["modified_at"])
    info.add_row("Custom metadata", json.dumps(doc["custom_metadata"]) if doc["custom_metadata"] else "(none)")
    console.print(info)

    if not doc["chunks"]:
        return
    table = Table(title="Chunks and storage locations")
    table.add_column("#", justify="right")
    table.add_column("Chunk ID", no_wrap=True)
    table.add_column("Size", justify="right")
    table.add_column("SHA-256")
    table.add_column("Status", no_wrap=True)
    table.add_column("Copies stored on")
    for chunk in doc["chunks"]:
        copies = []
        for r in chunk["replicas"]:
            if r["worker_state"] != "ALIVE":
                copies.append(f"{r['worker_id']} ({styled(r['worker_state'])})")
            elif r["replica_state"] != "HEALTHY":
                copies.append(f"{r['worker_id']} ({styled(r['replica_state'])})")
            else:
                copies.append(f"[green]{r['worker_id']}[/]")
        table.add_row(
            str(chunk["index"]), chunk["chunk_id"], str(chunk["size"]), chunk["checksum"][:10] + "...",
            styled(chunk["status"]), ", ".join(copies) or "(none)",
        )
    console.print(table)


# -- commands ----------------------------------------------------------------
def cmd_upload(args) -> None:
    if not os.path.isfile(args.file):
        fail(f"file not found: {args.file}")
    name = args.name or os.path.basename(args.file)
    content_type = args.content_type or mimetypes.guess_type(name)[0] or "application/octet-stream"
    data = {}
    custom = parse_pairs(args.meta)
    if custom:
        data["metadata"] = json.dumps(custom)
    with open(args.file, "rb") as f:
        resp = request("POST", f"{API}/objects", 201, files={"file": (name, f, content_type)}, data=data)
    doc = resp.json()
    if args.json:
        print_json(doc)
    else:
        console.print("[bold green]Uploaded.[/]")
        show_object(doc)


def _download(object_id: str, sink) -> tuple[int, str, httpx.Response]:
    """Stream an object into `sink` (a binary file or None). Returns (bytes, sha256, response)."""
    digest = hashlib.sha256()
    received = 0
    try:
        with http() as client, client.stream("GET", f"{API}/objects/{object_id}") as resp:
            if resp.status_code != 200:
                resp.read()
                check(resp, 200)
            for block in resp.iter_bytes():
                digest.update(block)
                received += len(block)
                if sink is not None:
                    sink.write(block)
            return received, digest.hexdigest(), resp
    except httpx.HTTPError as exc:
        fail(f"download was cut short after {received} bytes ({type(exc).__name__}); no valid data was accepted")


def cmd_download(args) -> None:
    meta = request("GET", f"{API}/objects/{args.object_id}/metadata", 200).json()
    output = args.output or meta["name"]
    with open(output, "wb") as sink:
        received, checksum, resp = _download(args.object_id, sink)
    if received != meta["size"] or checksum != meta["checksum"]:
        os.remove(output)
        fail(f"downloaded data does not match the stored checksum (got {received} bytes, sha256 {checksum})")
    console.print(f"[bold green]Downloaded[/] {received} bytes to {output}")
    console.print(f"Content-Type: {resp.headers.get('content-type')}")
    console.print(f"SHA-256 {checksum} [green]matches the stored checksum[/]")


def cmd_info(args) -> None:
    doc = request("GET", f"{API}/objects/{args.object_id}/metadata", 200).json()
    print_json(doc) if args.json else show_object(doc)


def cmd_set_meta(args) -> None:
    patch = parse_pairs(args.pairs)
    for key in args.unset or []:
        patch[key] = None
    if not patch:
        fail("nothing to change: give key=value pairs and/or --unset key")
    doc = request("PATCH", f"{API}/objects/{args.object_id}/metadata", 200, json=patch).json()
    if args.json:
        print_json(doc)
    else:
        console.print("[bold green]Metadata updated[/] (object data was not re-sent).")
        show_object(doc)


def cmd_delete(args) -> None:
    request("DELETE", f"{API}/objects/{args.object_id}", 204)
    console.print(f"[bold green]Deleted[/] {args.object_id}")


def cmd_list(args) -> None:
    data = request("GET", f"{API}/objects", 200, params={"limit": args.limit, "offset": args.offset}).json()
    if args.json:
        print_json(data)
        return
    table = Table(title=f"Objects {data['offset'] + 1 if data['objects'] else 0}-"
                        f"{data['offset'] + len(data['objects'])} of {data['total']}")
    for column in ("Object ID", "Name", "Size", "Content type", "Chunks", "Created"):
        table.add_column(column)
    for obj in data["objects"]:
        table.add_row(obj["object_id"], obj["name"], human(obj["size"]), obj["content_type"],
                      str(obj["chunk_count"]), obj["created_at"])
    console.print(table)


def cmd_cluster(args) -> None:
    data = request("GET", f"{API}/admin/cluster", 200).json()
    if args.json:
        print_json(data)
        return
    table = Table(title="Storage workers")
    for column in ("Worker", "State", "Last heartbeat", "Copies held", "Used"):
        table.add_column(column)
    for w in data["workers"]:
        beat = "never" if w["seconds_since_heartbeat"] is None else f"{w['seconds_since_heartbeat']} s ago"
        table.add_row(w["worker_id"], styled(w["state"]), beat, str(w["recorded_replicas"]), human(w["used_bytes"]))
    console.print(table)

    rep = data["replication"]
    summary = Table(title=f"Replication (required copies per chunk: {rep['replication_factor']})")
    for column in ("Objects", "Chunks", "Healthy", "Under-replicated", "Unavailable", "Over-replicated", "Corrupt copies"):
        summary.add_column(column)
    summary.add_row(
        str(rep["objects"]), str(rep["chunks"]), f"[green]{rep['healthy']}[/]",
        f"[yellow]{rep['under_replicated']}[/]" if rep["under_replicated"] else "0",
        f"[bold red]{rep['unavailable']}[/]" if rep["unavailable"] else "0",
        f"[cyan]{rep['over_replicated']}[/]" if rep["over_replicated"] else "0",
        f"[bold red]{rep['corrupt_replicas']}[/]" if rep["corrupt_replicas"] else "0",
    )
    console.print(summary)
    if rep["fully_replicated"]:
        console.print("[bold green]Replication factor is maintained for every chunk.[/]")
    else:
        console.print("[bold yellow]Replication factor is NOT maintained for every chunk.[/]")


def cmd_verify(args) -> None:
    """Download the stored object and compare it, byte for byte by SHA-256, with a local file."""
    if not os.path.isfile(args.file):
        fail(f"file not found: {args.file}")
    local = file_sha256(args.file)
    received, remote, _ = _download(args.object_id, None)
    console.print(f"local file : {local}  ({os.path.getsize(args.file)} bytes)")
    console.print(f"downloaded : {remote}  ({received} bytes)")
    if local != remote:
        fail("MISMATCH: the stored object differs from the local file")
    console.print("[bold green]MATCH: the retrieved object is identical to the original.[/]")


def cmd_gen(args) -> None:
    size = parse_size(args.size)
    with open(args.path, "wb") as f:
        remaining = size
        while remaining > 0:
            block = os.urandom(min(remaining, 1024 * 1024))
            f.write(block)
            remaining -= len(block)
    console.print(f"Created {args.path}: {size} bytes of random data, sha256 {file_sha256(args.path)}")


def cmd_sha256(args) -> None:
    print(f"{file_sha256(args.path)}  {args.path}")


# -- argument parsing --------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="objstore", description="Client for the Mini Distributed Cloud Object Store")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("upload", help="upload a file as a new object")
    p.add_argument("file")
    p.add_argument("--meta", "-m", action="append", metavar="KEY=VALUE", help="custom metadata (repeatable)")
    p.add_argument("--name", help="object name (default: the file name)")
    p.add_argument("--content-type", help="content type (default: guessed from the name)")
    p.add_argument("--json", action="store_true", help="print the raw JSON response")
    p.set_defaults(func=cmd_upload)

    p = sub.add_parser("download", help="download an object and verify its checksum")
    p.add_argument("object_id")
    p.add_argument("--output", "-o", help="where to save it (default: the object's name)")
    p.set_defaults(func=cmd_download)

    p = sub.add_parser("info", help="show metadata, chunks and storage locations")
    p.add_argument("object_id")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_info)

    p = sub.add_parser("set-meta", help="add, update or remove custom metadata without re-uploading")
    p.add_argument("object_id")
    p.add_argument("pairs", nargs="*", metavar="KEY=VALUE")
    p.add_argument("--unset", action="append", metavar="KEY", help="remove a key (repeatable)")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_set_meta)

    p = sub.add_parser("delete", help="delete an object and its stored data")
    p.add_argument("object_id")
    p.set_defaults(func=cmd_delete)

    p = sub.add_parser("list", help="list stored objects")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("cluster", help="show worker health and replication status")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_cluster)

    p = sub.add_parser("verify", help="compare a stored object with a local file")
    p.add_argument("object_id")
    p.add_argument("file")
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("gen", help="create a test file of random bytes, e.g. gen 5.5MB big.bin")
    p.add_argument("size")
    p.add_argument("path")
    p.set_defaults(func=cmd_gen)

    p = sub.add_parser("sha256", help="print the SHA-256 of a local file")
    p.add_argument("path")
    p.set_defaults(func=cmd_sha256)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
