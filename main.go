// FileWatcher extracts a chosen file type from ZIP archives that appear in a
// watched directory.
//
// It is written for the unattended case, which is what makes it more than a
// call to archive/zip. Three things go wrong in that setting and each one
// shapes the code below:
//
//   - an archive is visible before it is complete, so opening it on the
//     filesystem event gives you a truncated ZIP;
//   - the same file can be announced twice, and the process can be restarted
//     at any point, so "have I already done this one" has to survive a crash;
//   - several workers run at once, so that question cannot be answered with a
//     read followed by a write.
//
// The answers, in order: wait for the size to stop changing, keep the record in
// SQLite rather than in memory, and let the database resolve the race with a
// single INSERT OR IGNORE.
package main

import (
	"archive/zip"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	_ "modernc.org/sqlite"
)

type Config struct {
	SourceDir            string `json:"source_dir"`
	DestinationDir       string `json:"destination_dir"`
	WatchExtension       string `json:"watch_extension"`
	InnerExtension       string `json:"inner_extension"`
	WorkerCount          int    `json:"worker_count"`
	RetryIntervalSeconds int    `json:"retry_interval_seconds"`
	MaxRetries           int    `json:"max_retries"`
	DatabasePath         string `json:"database_path"`
}

func loadConfig(path string) (Config, error) {
	var cfg Config

	f, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer f.Close()

	err = json.NewDecoder(f).Decode(&cfg)
	return cfg, err
}

// resolveConfigPath looks for config.json beside the executable rather than in
// the working directory. A service manager decides the working directory and
// rarely picks the one you expect, so anchoring to the binary makes a manual
// run and a service run behave identically.
func resolveConfigPath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	exeDir := filepath.Dir(exe)
	return filepath.Join(exeDir, "config.json"), nil
}

func resolveMaybeRelativeToExe(p string) (string, error) {
	if filepath.IsAbs(p) {
		return p, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(exe), p), nil
}

type dbOp func(*sql.DB)

func initDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	// One connection, and one writer goroutine feeding it. SQLite permits a
	// single writer at a time; serialising here means the workers never meet
	// SQLITE_BUSY and never need retry logic of their own.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// WAL lets the ledger be read while a write is in flight; busy_timeout is
	// the safety net for the case this design is meant to prevent anyway.
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA synchronous=NORMAL;"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		return nil, err
	}

	createTable := `
CREATE TABLE IF NOT EXISTS processed_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  file_path TEXT UNIQUE,
  claimed_at DATETIME,
  processed_at DATETIME,
  status TEXT,
  extracted_file TEXT,
  error TEXT
);`
	if _, err := db.Exec(createTable); err != nil {
		return nil, err
	}

	return db, nil
}

func dbWriter(db *sql.DB, q <-chan dbOp) {
	for op := range q {
		op(db)
	}
}

// tryClaim records the file as ours, and reports whether we won it.
//
// The correctness of the whole program rests on this being one statement.
// A SELECT followed by an INSERT would leave a window in which two workers
// both see "not present" and both proceed; INSERT OR IGNORE against a UNIQUE
// column collapses that to a single atomic decision, and RowsAffected tells us
// which side of it we are on. It also survives restarts for free: a completed
// file is still in the table, so it is never claimed twice.
func tryClaim(q chan<- dbOp, path string) bool {
	ch := make(chan bool, 1)

	q <- func(db *sql.DB) {
		res, err := db.Exec(`
INSERT OR IGNORE INTO processed_files
  (file_path, claimed_at, status, extracted_file, error)
VALUES
  (?, datetime('now'), 'PROCESSING', '', '')`,
			path,
		)
		if err != nil {
			log.Println("DB claim error:", err)
			ch <- false
			return
		}

		n, _ := res.RowsAffected()
		ch <- (n == 1)
	}

	return <-ch
}

func finishResult(q chan<- dbOp, path, status, extracted, errMsg string) {
	q <- func(db *sql.DB) {
		_, err := db.Exec(`
UPDATE processed_files
SET processed_at = datetime('now'),
    status = ?,
    extracted_file = ?,
    error = ?
WHERE file_path = ?`,
			status, extracted, errMsg, path,
		)
		if err != nil {
			log.Println("DB update error:", err)
		}
	}
}

func main() {
	wd, _ := os.Getwd()
	exe, _ := os.Executable()
	log.Println("CWD:", wd)
	log.Println("EXE:", exe)

	configPath, err := resolveConfigPath()
	if err != nil {
		log.Fatal("Config path error:", err)
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatal("Config error:", err)
	}

	// Same reasoning as the config path: anchored to the binary, so the ledger
	// does not move when the working directory does.
	cfg.DatabasePath, err = resolveMaybeRelativeToExe(cfg.DatabasePath)
	if err != nil {
		log.Fatal("DB path resolve error:", err)
	}

	// Created up front so a missing folder is a startup error rather than a
	// failure on the first file to arrive, possibly days later.
	if err := os.MkdirAll(cfg.SourceDir, 0755); err != nil {
		log.Fatal("SourceDir mkdir error:", err)
	}
	if err := os.MkdirAll(cfg.DestinationDir, 0755); err != nil {
		log.Fatal("DestinationDir mkdir error:", err)
	}

	log.Println("Config:")
	log.Println("  SourceDir:", cfg.SourceDir)
	log.Println("  DestinationDir:", cfg.DestinationDir)
	log.Println("  WatchExtension:", cfg.WatchExtension)
	log.Println("  InnerExtension:", cfg.InnerExtension)
	log.Println("  WorkerCount:", cfg.WorkerCount)
	log.Println("  DatabasePath:", cfg.DatabasePath)

	db, err := initDB(cfg.DatabasePath)
	if err != nil {
		log.Fatal("DB error:", err)
	}
	defer db.Close()

	dbQ := make(chan dbOp, 1000)
	go dbWriter(db, dbQ)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal("Watcher create error:", err)
	}
	defer watcher.Close()

	if err := watcher.Add(cfg.SourceDir); err != nil {
		log.Fatal("Watcher add error:", err)
	}
	log.Println("Watching:", cfg.SourceDir)

	jobs := make(chan string, 1000)

	workerCount := cfg.WorkerCount
	if workerCount <= 0 {
		workerCount = 1
	}
	for i := 0; i < workerCount; i++ {
		go worker(i, jobs, cfg, dbQ)
	}

	// The watcher runs in its own goroutine and only enqueues paths; all the
	// slow work happens in the pool, so a burst of arrivals is never missed
	// while an archive is being unzipped.
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				// Rename matters as much as Create: the safe way to deliver a
				// file is to write it under a temporary name and rename it into
				// place, and that arrives as a rename, not a create.
				if (event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Rename == fsnotify.Rename) &&
					strings.HasSuffix(strings.ToLower(event.Name), strings.ToLower(cfg.WatchExtension)) {
					log.Println("Detected:", event.Name)
					jobs <- event.Name
				}

			case err := <-watcher.Errors:
				log.Println("Watcher error:", err)
			}
		}
	}()

	select {}
}

func worker(id int, jobs <-chan string, cfg Config, dbQ chan<- dbOp) {
	for path := range jobs {
		log.Printf("[Worker %d] Processing: %s\n", id, path)
		processZip(path, cfg, dbQ)
	}
}

func processZip(path string, cfg Config, dbQ chan<- dbOp) {
	if !tryClaim(dbQ, path) {
		log.Println("Skipping already claimed/processed:", path)
		return
	}

	interval := time.Duration(cfg.RetryIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 2 * time.Second
	}

	if err := waitUntilReady(path, cfg.MaxRetries, interval); err != nil {
		finishResult(dbQ, path, "FAILED", "", err.Error())
		return
	}

	reader, err := zip.OpenReader(path)
	if err != nil {
		finishResult(dbQ, path, "FAILED", "", err.Error())
		return
	}
	defer reader.Close()

	extractedFile := ""

	for _, f := range reader.File {
		if strings.HasSuffix(strings.ToLower(f.Name), strings.ToLower(cfg.InnerExtension)) {
			if err := extractFile(f, cfg.DestinationDir); err != nil {
				finishResult(dbQ, path, "FAILED", "", err.Error())
				return
			}
			// Every match is extracted, not just the first. An archive holding
			// two matching entries is unusual enough that silently dropping one
			// would be the worse surprise.
			extractedFile = f.Name
		}
	}

	if extractedFile == "" {
		finishResult(dbQ, path, "NO_MATCH", "", "")
	} else {
		finishResult(dbQ, path, "SUCCESS", extractedFile, "")
	}
}

// waitUntilReady blocks until the file has stopped growing.
//
// A filesystem event fires when the file appears, not when the writer is
// finished with it, so a ZIP opened at that moment is usually truncated. Two
// consecutive checks reporting the same size is the cheap approximation of
// "the writer has let go" — imperfect, since a slow writer could pause across
// both checks, but wrong far less often than not waiting at all.
//
// A file that does not exist yet is retried rather than treated as an error:
// on Windows the rename can land a moment after the event.
func waitUntilReady(path string, maxRetries int, interval time.Duration) error {
	if maxRetries <= 0 {
		maxRetries = 5
	}

	var lastSize int64 = -1

	for i := 0; i < maxRetries; i++ {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				time.Sleep(interval)
				continue
			}
			return err
		}

		if info.Size() == lastSize {
			return nil
		}

		lastSize = info.Size()
		time.Sleep(interval)
	}

	return errors.New("file not stable")
}

func extractFile(f *zip.File, destDir string) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("mkdir dest: %w", err)
	}

	// filepath.Base discards any directory part of the entry name, which is
	// what stops a crafted archive containing "../../etc/passwd" from writing
	// outside destDir. Flattening is deliberate, not incidental.
	outPath := filepath.Join(destDir, filepath.Base(f.Name))
	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, rc)
	return err
}
