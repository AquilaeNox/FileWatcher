# FileWatcher

[![CI](https://github.com/AquilaeNox/FileWatcher/actions/workflows/ci.yml/badge.svg)](https://github.com/AquilaeNox/FileWatcher/actions/workflows/ci.yml)

Watches a directory for incoming ZIP archives and extracts a chosen file type
from each one.

It is built for the unattended case — a folder that something else drops files
into, on a machine nobody is watching. That means the awkward parts are the
point: an archive may still be half-written when it appears, the same file may
arrive twice, and the process may be restarted at any moment. FileWatcher keeps
a small SQLite ledger so every archive is processed exactly once, and waits for
a file to stop growing before opening it.

Written in Go. SQLite is the pure-Go build, so there is no cgo and no system
library to install — the result is a single binary.

## How it works

```
   drop folder                    workers                  destination
  ┌────────────┐   fsnotify   ┌──────────────┐           ┌────────────┐
  │  *.zip     │ ───────────► │  claim → wait │ ────────► │  *.ddd     │
  └────────────┘              │  → unzip      │           └────────────┘
                              └───────┬───────┘
                                      │  every outcome recorded
                                 ┌────▼─────┐
                                 │  SQLite  │
                                 └──────────┘
```

1. **Detect** — `fsnotify` reports a new or renamed file matching
   `watch_extension`.
2. **Claim** — the worker inserts the path into SQLite with
   `INSERT OR IGNORE`. Exactly one worker can win that insert, so two workers
   can never process the same archive, and a restart will not reprocess a file
   that already completed.
3. **Wait** — the file is polled until its size stops changing. A ZIP that is
   still being copied is not a valid ZIP, and opening it early is the most
   common way this kind of tool fails.
4. **Extract** — every entry matching `inner_extension` is written to
   `destination_dir`. Entry names are flattened with `filepath.Base`, so a
   crafted archive cannot write outside the destination.
5. **Record** — the result is stored as `SUCCESS`, `NO_MATCH` or `FAILED`,
   with the error text if there was one.

## Configuration

Copy the example and edit it:

```sh
cp config.example.json config.json
```

The binary reads `config.json` **from its own directory**, not from the
current working directory, so it behaves the same when launched by hand or by
a service manager.

```json
{
  "source_dir": "C:\\notify\\input",
  "destination_dir": "C:\\notify\\output",
  "watch_extension": ".zip",
  "inner_extension": ".ddd",
  "worker_count": 4,
  "retry_interval_seconds": 2,
  "max_retries": 5,
  "database_path": ".\\db.sqlite"
}
```

| Key | Meaning |
| --- | --- |
| `source_dir` | directory to watch; created if missing |
| `destination_dir` | where extracted files are written; created if missing |
| `watch_extension` | archive extension that triggers processing |
| `inner_extension` | which files inside the archive to extract |
| `worker_count` | archives processed in parallel |
| `retry_interval_seconds` | pause between size checks while waiting for a file |
| `max_retries` | how many checks before giving up on an unstable file |
| `database_path` | SQLite ledger; relative paths resolve next to the binary |

## Build and run

```sh
go build -o FileWatcher .
./FileWatcher
```

Requires Go 1.25 or newer (a dependency sets that floor).

## The ledger

```sql
SELECT file_path, status, extracted_file, processed_at
FROM processed_files
ORDER BY id DESC LIMIT 20;
```

| status | meaning |
| --- | --- |
| `PROCESSING` | claimed, not finished — or interrupted mid-run |
| `SUCCESS` | a matching file was extracted |
| `NO_MATCH` | the archive was readable but contained nothing matching |
| `FAILED` | unreadable, or never stopped changing; see the `error` column |

## Known limitations

Honest list, in rough order of how likely you are to hit them:

- **Files already present at startup are ignored.** Only filesystem events
  trigger work, so anything sitting in the folder before launch waits for the
  next event.
- **A crash leaves rows in `PROCESSING`.** Because the claim is what prevents
  reprocessing, an interrupted archive is never retried. Recovering those rows
  on startup would fix it.
- **No graceful shutdown.** The process runs until killed, so an extraction can
  be cut off mid-write.
- **The watcher is not recursive** — subdirectories of `source_dir` are not
  watched.

## License

MIT — see [LICENSE](LICENSE).
