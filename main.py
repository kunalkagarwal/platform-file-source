"""Platform File Source (Storage v2).

A source node (no upstream input). It logs in to the platform as the user,
downloads ONE file from a dataset's databank through the platform's file
server, and writes it as Parquet for the next nodes.

Flow (the same calls the platform's own website makes):
  1. username / password come from the user's stored secrets (env.get_secret);
  2. POST <login>            grant_type=password  -> short-lived access token;
  3. POST <files>/v1/databanks/{id}/files          -> list the databank's files;
  4. POST <files>/v1/databanks/{id}/files/download -> temporary download link;
  5. GET  <that link>                              -> the file's bytes;
  6. convert CSV / JSON to Parquet (Parquet is kept as it is) and upload.

The password and the token are never logged. The password and username are
also registered for log masking by the SDK when fetched.
"""

import json
import os
import shutil
import urllib.error
import urllib.parse
import urllib.request

import duckdb
from pipeline_sdk import context, env
from pipeline_sdk.context import log

NODE_VERSION = "2026-10-27.2"

DEFAULT_TOKEN_URL = "https://platform.iudx.io/auth/realms/iudx/protocol/openid-connect/token"
DEFAULT_FILE_SERVER = "https://platform.iudx.io/files-connect-api/v1"
# The client the platform's own website logs in with (public client, user login).
CLIENT_ID = "angular-client"
DEFAULT_USERNAME_SECRET = "PLATFORM_USERNAME"
DEFAULT_PASSWORD_SECRET = "PLATFORM_PASSWORD"

MAX_DOWNLOAD_BYTES = 2 * 1024 * 1024 * 1024  # refuse anything bigger than 2 GiB
LIST_LIMIT = 1000
SUPPORTED_EXTENSIONS = (".csv", ".json", ".jsonl", ".ndjson", ".geojson", ".parquet")
MAX_GEOJSON_BYTES = 500 * 1024 * 1024  # a GeoJSON file is read into memory, so it has a lower limit


class PlatformError(Exception):
    """A problem the user can act on; the message is safe to show."""


def _request(method, url, *, json_body=None, form=None, token=None, timeout=60):
    """One HTTP call; returns (status, parsed JSON or {}). Never raises on an HTTP error status."""
    headers = {}
    data = None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif json_body is not None:
        data = json.dumps(json_body).encode()
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            status = response.status
    except urllib.error.HTTPError as error:
        raw = error.read()
        status = error.code
    except urllib.error.URLError as error:
        raise PlatformError(f"Could not reach {urllib.parse.urlparse(url).netloc}: {error.reason}") from error
    try:
        return status, json.loads(raw or b"{}")
    except ValueError:
        return status, {}


def login(token_url, username, password):
    """Password login; returns an access token. The password never appears in an error."""
    status, body = _request(
        "POST",
        token_url,
        form={"grant_type": "password", "client_id": CLIENT_ID, "username": username, "password": password},
    )
    if status == 200 and isinstance(body.get("access_token"), str):
        return body["access_token"]
    reason = body.get("error_description") or body.get("error") or f"HTTP {status}"
    raise PlatformError(
        f"Could not log in to the platform ({reason}). "
        "Check that the username and password secrets hold your own platform login."
    )


class FileServer:
    """The file server, with one automatic re-login when the token has expired."""

    def __init__(self, base_url, token_url, username, password):
        self.base = base_url.rstrip("/")
        self.token_url = token_url
        self._username = username
        self._password = password
        self._token = login(token_url, username, password)

    def _post(self, path, body):
        for attempt in (1, 2):
            status, answer = _request("POST", f"{self.base}{path}", json_body=body, token=self._token)
            if status == 401 and attempt == 1:
                log("The access token expired; logging in again")
                self._token = login(self.token_url, self._username, self._password)
                continue
            return status, answer
        raise AssertionError("unreachable")

    def list_files(self, databank_id):
        status, answer = self._post(
            f"/databanks/{databank_id}/files",
            {"prefix": "", "maxKeys": LIST_LIMIT, "delimiter": "/", "recursive": True},
        )
        if status == 403:
            raise PlatformError("The platform says you are not allowed to read this dataset.")
        if status != 200:
            raise PlatformError(f"The file server could not list the files (HTTP {status}).")
        files = (answer.get("data") or {}).get("files") or []
        return [item for item in files if isinstance(item, dict) and item.get("key")]

    def download_link(self, databank_id, key):
        status, answer = self._post(f"/databanks/{databank_id}/files/download", {"key": key, "presigned": True})
        if status == 403:
            raise PlatformError("The platform says you are not allowed to download this file.")
        if status == 404:
            raise PlatformError(f"The file '{key}' was not found in the dataset.")
        link = (answer.get("data") or {}).get("presignedUrl")
        if status != 200 or not link:
            raise PlatformError(f"The file server did not give a download link (HTTP {status}).")
        return link


def choose_file(files, wanted_key):
    """The file to download: the one asked for, or the only one."""
    if not files:
        raise PlatformError(
            "The dataset has no files on the file server. Check the dataset ID: "
            "a dataset can exist on the platform without its files being stored there."
        )
    if wanted_key:
        wanted = wanted_key.strip().lstrip("/")
        for item in files:
            if item["key"] == wanted:
                return item
        names = ", ".join(item["key"] for item in files[:10])
        raise PlatformError(f"No file '{wanted}' in the dataset. Files found: {names}")
    if len(files) > 1:
        names = ", ".join(item["key"] for item in files[:10])
        more = f" (and {len(files) - 10} more)" if len(files) > 10 else ""
        raise PlatformError(
            f"The dataset has {len(files)} files, so say which one to take in 'File path in the dataset'. "
            f"Files found: {names}{more}"
        )
    return files[0]


def download(link, destination):
    """Stream the (temporary, pre-signed) link to a local file; returns the size in bytes."""
    request = urllib.request.Request(link, method="GET")
    size = 0
    try:
        with urllib.request.urlopen(request, timeout=120) as response, open(destination, "wb") as out:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_DOWNLOAD_BYTES:
                    raise PlatformError("The file is larger than 2 GB, which this node does not load.")
                out.write(chunk)
    except urllib.error.HTTPError as error:
        raise PlatformError(f"The download link did not work (HTTP {error.code}); it may have expired.") from error
    except urllib.error.URLError as error:
        raise PlatformError(f"Could not download the file: {error.reason}") from error
    return size


def geojson_to_rows(source, destination):
    """Turn a GeoJSON file into one JSON line per feature: the feature's properties as columns
    plus a `geometry` column that holds the shape as GeoJSON text."""
    if os.path.getsize(source) > MAX_GEOJSON_BYTES:
        raise PlatformError("This GeoJSON file is larger than 500 MB, which this node does not load.")
    try:
        with open(source, encoding="utf-8-sig") as handle:
            document = json.load(handle)
    except ValueError as error:
        raise PlatformError(f"The file is not valid GeoJSON: {error}") from error
    kind = document.get("type") if isinstance(document, dict) else None
    if kind == "FeatureCollection":
        features = document.get("features") or []
    elif kind == "Feature":
        features = [document]
    else:
        raise PlatformError("The file is not a GeoJSON FeatureCollection or Feature.")
    with open(destination, "w", encoding="utf-8") as out:
        for feature in features:
            if not isinstance(feature, dict):
                continue
            row = dict(feature.get("properties") or {})
            if "geometry" in row:
                row["properties_geometry"] = row.pop("geometry")
            geometry = feature.get("geometry")
            row["geometry"] = json.dumps(geometry, separators=(",", ":")) if geometry is not None else None
            out.write(json.dumps(row) + "\n")
    return len(features)


def to_parquet(source, extension, destination):
    """Write `source` as Parquet at `destination`; returns the row count."""
    if extension == ".geojson":
        lines = source + ".rows.jsonl"
        if geojson_to_rows(source, lines) == 0:
            raise PlatformError("The GeoJSON file has no features.")
        source, extension = lines, ".jsonl"
    if extension == ".parquet":
        shutil.copyfile(source, destination)
        reader = "read_parquet"
    elif extension == ".csv":
        reader = "read_csv_auto"
    elif extension in (".json", ".jsonl", ".ndjson"):
        reader = "read_json_auto"
    else:
        raise PlatformError(
            f"Files of type '{extension or 'unknown'}' are not supported yet. "
            f"Supported: {', '.join(SUPPORTED_EXTENSIONS)}."
        )
    connection = duckdb.connect()
    try:
        escaped_source = source.replace("'", "''")
        if extension != ".parquet":
            escaped_destination = destination.replace("'", "''")
            try:
                options = ", sample_size=-1" if reader == "read_json_auto" else ""
                connection.execute(
                    f"COPY (SELECT * FROM {reader}('{escaped_source}'{options})) TO '{escaped_destination}' (FORMAT PARQUET)"
                )
            except duckdb.Error as error:
                raise PlatformError(f"The file could not be read as {extension[1:].upper()}: {error}") from error
        escaped_destination = destination.replace("'", "''")
        return connection.execute(f"SELECT COUNT(*) FROM read_parquet('{escaped_destination}')").fetchone()[0]
    finally:
        connection.close()


def _text(config, name, default=""):
    return str(config.get(name) or default).strip()


def handler(ctx: context.NodeContext, workdir: str):
    log(f"Platform file source, version {NODE_VERSION}")
    databank_id = _text(ctx.config, "databank_id")
    if not databank_id:
        raise PlatformError("'Dataset (databank) ID' is required.")
    username_secret = _text(ctx.config, "username_secret", DEFAULT_USERNAME_SECRET)
    password_secret = _text(ctx.config, "password_secret", DEFAULT_PASSWORD_SECRET)
    token_url = _text(ctx.config, "token_url", DEFAULT_TOKEN_URL)
    file_server = _text(ctx.config, "file_server_url", DEFAULT_FILE_SERVER)

    log(f"Reading the secrets '{username_secret}' and '{password_secret}'")
    # A stray space or line break from copy and paste would make the login fail without being visible.
    username = env.get_secret(username_secret).strip()
    password = env.get_secret(password_secret).rstrip("\r\n")
    if "@" not in username:
        log(f"Note: the secret '{username_secret}' does not look like an email address")

    log(f"Logging in to the platform as the user in '{username_secret}'")
    server = FileServer(file_server, token_url, username, password)

    log(f"Listing the files of dataset {databank_id}")
    files = server.list_files(databank_id)
    chosen = choose_file(files, _text(ctx.config, "file_key"))
    key = chosen["key"]
    log(f"Downloading '{key}' ({chosen.get('size', '?')} bytes)")

    extension = os.path.splitext(key)[1].lower()
    raw_path = os.path.join(workdir, "downloaded" + extension)
    size = download(server.download_link(databank_id, key), raw_path)
    log(f"Downloaded {size} bytes")

    result_path = os.path.join(workdir, "result.parquet")
    rows = to_parquet(raw_path, extension, result_path)
    log(f"Converted to Parquet: {rows} row(s)")

    for out in ctx.outputs:
        log(f"Uploading to {out.path}")
        out.upload_file(result_path)
    return {"databankId": databank_id, "file": key, "bytes": size, "rows": rows}


if __name__ == "__main__":
    context.run(handler, prefix="[PLATFORM FILE]")
