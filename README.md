# Platform File Source

A source node for the no-code editor. It logs in to the platform **as you**, downloads one
file from a dataset (databank) through the platform's file server, and writes it as Parquet
so the next nodes (SQL, Superset, ...) can use it.

## Use it

1. In the editor, profile menu → Settings → Secrets, create two secrets (uppercase names):
   - `PLATFORM_USERNAME` – your platform login (email)
   - `PLATFORM_PASSWORD` – your platform password
2. Add the node, then fill in:
   - **Dataset (databank) ID** – the id in the dataset's page address.
   - **File path in the dataset** – needed only when the dataset has more than one file.
3. Run. The output is `result.parquet`.

Supported files: CSV, JSON / JSON-lines, Parquet. Other types (for example GeoPackage) give a
clear "not supported yet" error. Files larger than 2 GB are refused.

## What it does

1. Reads the two secrets (the SDK masks their values in the logs).
2. Password login against the platform's login server (`angular-client`, realm `iudx`),
   which returns a token that lasts about 5 minutes. If the token expires mid-run the node
   logs in once more.
3. `POST /v1/databanks/{id}/files` – lists files; `POST /v1/databanks/{id}/files/download`
   – gets a temporary download link; the file is streamed from that link.
4. CSV / JSON are converted with DuckDB; Parquet is kept as it is; the result is uploaded
   through the editor's storage (the pod holds no storage credentials).

The password and token are never logged or written to the output.

## Config

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | Node name (snake_case) |
| `databank_id` | yes | Dataset id |
| `file_key` | no | File path inside the dataset |
| `username_secret` / `password_secret` | no | Names of the secrets (defaults above) |
| `file_server_url` / `token_url` | no | Override the platform addresses (for other deployments) |

## Tests

The tests run against a mock login and file server and need no network:

```
docker build -t platform-file-source:test .
docker run --rm -v "$PWD/tests:/app/tests:ro" platform-file-source:test python -m unittest discover -s tests -v
```

## Build on the platform

Push this folder to a **public** GitHub repo, edit `githubUrl` in `image.json`, then use
Images → build (repo, ref, `Dockerfile`, context `.`), and Nodes → Add Node with `node.json`.
