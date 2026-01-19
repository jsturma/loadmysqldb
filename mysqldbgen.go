package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faker/faker/v4"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	DBName          string `yaml:"dbname"`
	ConfigPath      string `yaml:"-"`
	RunOnlyFaker    bool   `yaml:"runOnlyFaker"`
	NumWorkers      int    `yaml:"numWorkers"`
	DBRecordsToLoad int    `yaml:"dbRecords2Process"`
	PcentOutput     int    `yaml:"pcentOutput"`
	MinDays         int64  `yaml:"minDays"`
	MaxDays         int64  `yaml:"maxDays"`
	DelayLastLogin  int64  `yaml:"delayLastLogin"`
}

func defaultConfig() Config {
	return Config{
		Host:            "localhost",
		Port:            3306,
		User:            "root",
		Password:        "root",
		DBName:          "mytestdb",
		RunOnlyFaker:    false,
		NumWorkers:      3,
		DBRecordsToLoad: 100,
		PcentOutput:     10,
		MinDays:         3 * 24 * 60 * 60,
		MaxDays:         365 * 24 * 60 * 60,
		DelayLastLogin:  500,
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfgPath := findConfigPath(os.Args[1:])
	cfg := defaultConfig()
	if cfgPath != "" {
		if err := loadYAML(cfgPath, &cfg); err != nil {
			log.Fatalf("failed to load config %q: %v", cfgPath, err)
		}
		cfg.ConfigPath = cfgPath
	}

	fs := flag.NewFlagSet(filepath.Base(os.Args[0]), flag.ExitOnError)
	fs.StringVar(&cfg.Host, "host", cfg.Host, "MySQL host")
	fs.IntVar(&cfg.Port, "port", cfg.Port, "MySQL port")
	fs.StringVar(&cfg.User, "user", cfg.User, "MySQL admin user")
	fs.StringVar(&cfg.Password, "password", cfg.Password, "MySQL admin password")
	fs.StringVar(&cfg.DBName, "dbname", cfg.DBName, "database to create/populate")
	fs.StringVar(&cfg.ConfigPath, "config", cfg.ConfigPath, "path to YAML config file (optional)")
	fs.BoolVar(&cfg.RunOnlyFaker, "runOnlyFaker", cfg.RunOnlyFaker, "generate fake data but do not write to DB")
	fs.IntVar(&cfg.NumWorkers, "numWorkers", cfg.NumWorkers, "number of concurrent workers inserting rows")
	fs.IntVar(&cfg.DBRecordsToLoad, "dbRecords2Process", cfg.DBRecordsToLoad, "number of logical records to create")
	fs.IntVar(&cfg.PcentOutput, "pcentOutput", cfg.PcentOutput, "progress output every X percent")
	fs.Int64Var(&cfg.MinDays, "minDays", cfg.MinDays, "minimum account created offset in seconds")
	fs.Int64Var(&cfg.MaxDays, "maxDays", cfg.MaxDays, "maximum account created offset in seconds")
	fs.Int64Var(&cfg.DelayLastLogin, "delayLastLogin", cfg.DelayLastLogin, "random last-login delay in seconds")
	_ = fs.Parse(os.Args[1:])

	if cfg.DBRecordsToLoad < 1 {
		log.Fatalf("dbRecords2Process must be >= 1")
	}
	if cfg.NumWorkers < 1 {
		log.Fatalf("numWorkers must be >= 1")
	}
	if cfg.PcentOutput < 1 || cfg.PcentOutput > 100 {
		log.Fatalf("pcentOutput must be 1..100")
	}
	if cfg.MaxDays < cfg.MinDays {
		log.Fatalf("maxDays must be >= minDays")
	}

	log.Printf("mysqldbgen: host=%s port=%d user=%s dbname=%s workers=%d records=%d config=%q runOnlyFaker=%v",
		cfg.Host, cfg.Port, cfg.User, cfg.DBName, cfg.NumWorkers, cfg.DBRecordsToLoad, cfg.ConfigPath, cfg.RunOnlyFaker)

	ctx := context.Background()
	if cfg.RunOnlyFaker {
		runOnlyFaker(ctx, cfg)
		return
	}

	if err := ensureDatabaseAndSchema(ctx, cfg); err != nil {
		log.Fatalf("setup failed: %v", err)
	}
	if err := loadData(ctx, cfg); err != nil {
		log.Fatalf("load failed: %v", err)
	}
	log.Printf("done")
}

func findConfigPath(args []string) string {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "-config" || a == "--config" {
			if i+1 < len(args) {
				return args[i+1]
			}
			return ""
		}
		if strings.HasPrefix(a, "-config=") {
			return strings.TrimPrefix(a, "-config=")
		}
		if strings.HasPrefix(a, "--config=") {
			return strings.TrimPrefix(a, "--config=")
		}
	}
	return ""
}

func loadYAML(path string, out *Config) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, out)
}

func ensureDatabaseAndSchema(ctx context.Context, cfg Config) error {
	// Connect without selecting a DB so we can create it if missing.
	adminDB, err := sql.Open("mysql", dsn(cfg, ""))
	if err != nil {
		return err
	}
	defer adminDB.Close()

	adminDB.SetConnMaxLifetime(5 * time.Minute)
	adminDB.SetMaxOpenConns(4)
	adminDB.SetMaxIdleConns(4)

	if err := adminDB.PingContext(ctx); err != nil {
		return err
	}

	if _, err := adminDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", cfg.DBName)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	db, err := sql.Open("mysql", dsn(cfg, cfg.DBName))
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS accounts (
			a_uuid CHAR(36) PRIMARY KEY,
			a_username VARCHAR(64) NOT NULL,
			a_email VARCHAR(255) NOT NULL,
			a_password VARCHAR(128) NOT NULL,
			a_created_epoch BIGINT NOT NULL,
			a_last_login_epoch BIGINT NOT NULL,
			INDEX idx_accounts_email (a_email),
			INDEX idx_accounts_created (a_created_epoch)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE IF NOT EXISTS products (
			pr_uuid CHAR(36) PRIMARY KEY,
			pr_name VARCHAR(255) NOT NULL,
			pr_authors VARCHAR(512) NOT NULL,
			pr_price DECIMAL(10,2) NOT NULL,
			INDEX idx_products_price (pr_price)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE IF NOT EXISTS payments (
			p_md5 CHAR(32) PRIMARY KEY,
			p_amount DECIMAL(10,2) NOT NULL,
			p_epoch BIGINT NOT NULL,
			INDEX idx_payments_epoch (p_epoch)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE IF NOT EXISTS buying_stats (
			bs_account_uuid CHAR(36) NOT NULL,
			bs_product_uuid CHAR(36) NOT NULL,
			bs_quantity INT NOT NULL,
			bs_total_amount DECIMAL(10,2) NOT NULL,
			bs_epoch BIGINT NOT NULL,
			INDEX idx_bs_epoch (bs_epoch),
			INDEX idx_bs_account (bs_account_uuid),
			INDEX idx_bs_product (bs_product_uuid),
			CONSTRAINT fk_bs_account FOREIGN KEY (bs_account_uuid) REFERENCES accounts(a_uuid) ON DELETE CASCADE,
			CONSTRAINT fk_bs_product FOREIGN KEY (bs_product_uuid) REFERENCES products(pr_uuid) ON DELETE CASCADE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}

	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("create tables: %w", err)
		}
	}
	return nil
}

func loadData(ctx context.Context, cfg Config) error {
	db, err := sql.Open("mysql", dsn(cfg, cfg.DBName))
	if err != nil {
		return err
	}
	defer db.Close()

	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetMaxOpenConns(max(4, cfg.NumWorkers*2))
	db.SetMaxIdleConns(max(4, cfg.NumWorkers*2))

	if err := db.PingContext(ctx); err != nil {
		return err
	}

	insAcct, err := db.PrepareContext(ctx, `INSERT INTO accounts (a_uuid,a_username,a_email,a_password,a_created_epoch,a_last_login_epoch)
		VALUES (?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer insAcct.Close()

	insProd, err := db.PrepareContext(ctx, `INSERT INTO products (pr_uuid,pr_name,pr_authors,pr_price)
		VALUES (?,?,?,?)`)
	if err != nil {
		return err
	}
	defer insProd.Close()

	insPay, err := db.PrepareContext(ctx, `INSERT INTO payments (p_md5,p_amount,p_epoch)
		VALUES (?,?,?)`)
	if err != nil {
		return err
	}
	defer insPay.Close()

	insBS, err := db.PrepareContext(ctx, `INSERT INTO buying_stats (bs_account_uuid,bs_product_uuid,bs_quantity,bs_total_amount,bs_epoch)
		VALUES (?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer insBS.Close()

	var inserted uint64
	recordsPerLog := recordsPerLog(cfg.DBRecordsToLoad, cfg.PcentOutput)
	start := time.Now()

	jobs := make(chan int, cfg.NumWorkers*4)
	var wg sync.WaitGroup
	errCh := make(chan error, cfg.NumWorkers)

	for w := 0; w < cfg.NumWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID*1000)))
			for range jobs {
				rec := generateRecord(r, cfg)

				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					errCh <- err
					return
				}

				if _, err := tx.StmtContext(ctx, insAcct).ExecContext(ctx,
					rec.AccountUUID, rec.Username, rec.Email, rec.Password, rec.CreatedEpoch, rec.LastLoginEpoch); err != nil {
					_ = tx.Rollback()
					if isDupKey(err) {
						continue
					}
					errCh <- err
					return
				}

				if _, err := tx.StmtContext(ctx, insProd).ExecContext(ctx,
					rec.ProductUUID, rec.ProductName, rec.ProductAuthors, rec.ProductPrice); err != nil {
					_ = tx.Rollback()
					if isDupKey(err) {
						continue
					}
					errCh <- err
					return
				}

				if _, err := tx.StmtContext(ctx, insPay).ExecContext(ctx,
					rec.PaymentMD5, rec.PaymentAmount, rec.PaymentEpoch); err != nil {
					_ = tx.Rollback()
					if isDupKey(err) {
						continue
					}
					errCh <- err
					return
				}

				if _, err := tx.StmtContext(ctx, insBS).ExecContext(ctx,
					rec.AccountUUID, rec.ProductUUID, rec.Quantity, rec.TotalAmount, rec.BuyingEpoch); err != nil {
					_ = tx.Rollback()
					if isDupKey(err) {
						continue
					}
					errCh <- err
					return
				}

				if err := tx.Commit(); err != nil {
					errCh <- err
					return
				}

				n := atomic.AddUint64(&inserted, 1)
				if recordsPerLog > 0 && int(n)%recordsPerLog == 0 {
					elapsed := time.Since(start)
					rps := float64(n) / math.Max(elapsed.Seconds(), 0.001)
					log.Printf("progress: %d/%d (%.1f%%) rate=%.0f rec/s elapsed=%s",
						n, cfg.DBRecordsToLoad, 100*float64(n)/float64(cfg.DBRecordsToLoad), rps, elapsed.Truncate(time.Millisecond))
				}
				if int(n) >= cfg.DBRecordsToLoad {
					return
				}
			}
		}(w)
	}

	for i := 0; i < cfg.DBRecordsToLoad; i++ {
		select {
		case err := <-errCh:
			return err
		default:
		}
		jobs <- i
	}
	close(jobs)

	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}

	final := atomic.LoadUint64(&inserted)
	if int(final) < cfg.DBRecordsToLoad {
		return fmt.Errorf("only inserted %d/%d records (duplicate keys likely); try increasing dbRecords2Process", final, cfg.DBRecordsToLoad)
	}

	elapsed := time.Since(start)
	rps := float64(final) / math.Max(elapsed.Seconds(), 0.001)
	log.Printf("inserted %d records in %s (%.0f rec/s)", final, elapsed.Truncate(time.Millisecond), rps)
	return nil
}

func recordsPerLog(total int, pcent int) int {
	if total <= 0 {
		return 0
	}
	step := int(math.Round(float64(total) * float64(pcent) / 100.0))
	if step < 1 {
		step = 1
	}
	return step
}

type Record struct {
	AccountUUID    string
	Username       string
	Email          string
	Password       string
	CreatedEpoch   int64
	LastLoginEpoch int64

	ProductUUID    string
	ProductName    string
	ProductAuthors string
	ProductPrice   float64

	PaymentMD5    string
	PaymentAmount float64
	PaymentEpoch  int64

	Quantity    int
	TotalAmount float64
	BuyingEpoch int64
}

func generateRecord(r *rand.Rand, cfg Config) Record {
	now := time.Now().Unix()
	createdAgo := randRangeInt64(r, cfg.MinDays, cfg.MaxDays)
	created := now - createdAgo
	lastLogin := created + randRangeInt64(r, 0, cfg.DelayLastLogin)

	price := round2(randRangeFloat(r, 1.0, 250.0))
	qty := int(randRangeInt64(r, 1, 6))
	total := round2(price * float64(qty))
	paymentAmount := total

	return Record{
		AccountUUID:    faker.UUIDHyphenated(),
		Username:       faker.Username(),
		Email:          faker.Email(),
		Password:       faker.Password(),
		CreatedEpoch:   created,
		LastLoginEpoch: lastLogin,

		ProductUUID:    faker.UUIDHyphenated(),
		ProductName:    faker.Word(),
		ProductAuthors: strings.Join([]string{faker.Name(), faker.Name()}, ", "),
		ProductPrice:   price,

		PaymentMD5:    randomMD5(r),
		PaymentAmount: paymentAmount,
		PaymentEpoch:  now - randRangeInt64(r, 0, 3600*24*30),

		Quantity:    qty,
		TotalAmount: total,
		BuyingEpoch: now - randRangeInt64(r, 0, 3600*24*30),
	}
}

func runOnlyFaker(_ context.Context, cfg Config) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	limit := min(cfg.DBRecordsToLoad, 10)
	for i := 0; i < limit; i++ {
		rec := generateRecord(r, cfg)
		log.Printf("faker[%d]: acct=%s email=%s product=%s price=%.2f qty=%d total=%.2f payment=%s",
			i, rec.AccountUUID, rec.Email, rec.ProductUUID, rec.ProductPrice, rec.Quantity, rec.TotalAmount, rec.PaymentMD5)
	}
	log.Printf("runOnlyFaker: generated %d sample records (set runOnlyFaker=false to load DB)", limit)
}

func dsn(cfg Config, dbname string) string {
	// Allow DB name to be empty so we can connect and run CREATE DATABASE.
	// parseTime helps with time scanning if users extend the schema later.
	// multiStatements=false by default.
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4,utf8&collation=utf8mb4_unicode_ci",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, dbname)
}

func isDupKey(err error) bool {
	// go-sql-driver/mysql returns *mysql.MySQLError, but we avoid importing the driver type
	// to keep dependencies minimal. The error string contains "Duplicate entry".
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Duplicate entry")
}

func randRangeInt64(r *rand.Rand, minv, maxv int64) int64 {
	if maxv <= minv {
		return minv
	}
	return minv + r.Int63n(maxv-minv+1)
}

func randRangeFloat(r *rand.Rand, minv, maxv float64) float64 {
	if maxv <= minv {
		return minv
	}
	return minv + (maxv-minv)*r.Float64()
}

func round2(f float64) float64 {
	return math.Round(f*100) / 100
}

func randomMD5(r *rand.Rand) string {
	var b [32]byte
	for i := 0; i < len(b); i++ {
		b[i] = byte(r.Intn(256))
	}
	sum := md5.Sum(b[:]) // #nosec G401 -- demo data only; used as a deterministic 32-char token
	return hex.EncodeToString(sum[:])
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
