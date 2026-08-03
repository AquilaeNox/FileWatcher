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

	// For SQLite it's best to keep a single connection in the pool.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Pragmas: WAL + busy_timeout helps under contention.
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

// tryClaim atomically claims a file for processing.
// If another worker already claimed it, it returns false (no race).
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

	// Resolve DB path relative to EXE if needed (service-safe).
	cfg.DatabasePath, err = resolveMaybeRelativeToExe(cfg.DatabasePath)
	if err != nil {
		log.Fatal("DB path resolve error:", err)
	}

	// Ensure directories exist (avoids startup failures).
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

	// Worker pool (parallel file/zip work).
	workerCount := cfg.WorkerCount
	if workerCount <= 0 {
		workerCount = 1
	}
	for i := 0; i < workerCount; i++ {
		go worker(i, jobs, cfg, dbQ)
	}

	// Watcher loop
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				// Many programs write a temp file then rename; Create is ok, but file may not exist yet.
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

// ---------------- WORKER ----------------
func worker(id int, jobs <-chan string, cfg Config, dbQ chan<- dbOp) {
	for path := range jobs {
		log.Printf("[Worker %d] Processing: %s\n", id, path)
		processZip(path, cfg, dbQ)
	}
}

// ---------------- PROCESSING ----------------
func processZip(path string, cfg Config, dbQ chan<- dbOp) {
	// Atomic claim eliminates check-then-insert race.
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
			extractedFile = f.Name
			// If you only ever want the first match, uncomment:
			// break
		}
	}

	if extractedFile == "" {
		finishResult(dbQ, path, "NO_MATCH", "", "")
	} else {
		finishResult(dbQ, path, "SUCCESS", extractedFile, "")
	}
}

// ---------------- FILE READY ----------------
// waitUntilReady waits until the file exists and its size stabilizes across checks.
// Key change: if the file does not exist yet, we retry instead of failing immediately
// (prevents the repeated GetFileAttributes "file not found" noise).
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

	// Ensure destination dir exists.
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("mkdir dest: %w", err)
	}

	outPath := filepath.Join(destDir, filepath.Base(f.Name))
	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, rc)
	return err
}
