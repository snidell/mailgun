package repo

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"os"
	"sync"
	"time"
)

var (
	postgresConfig struct {
		Host      string
		Port      string
		User      string
		Password  string
		Cert      string
		DB        string
		Timeout   time.Duration
		KeepAlive time.Duration
		Pool      int
	}
	once    sync.Once
	db      DB
	dbError error
)

type DB struct {
	Session *pgx.Conn
}

// Connect to DB
func postgresqlConnConfig() (c *pgx.Conn, err error) {
	localURL := "postgres://postgres:Pass2020!@localhost:6001/postgres"
	conn, err := pgx.Connect(context.Background(), localURL)
	if err != nil {
		fmt.Println(os.Stderr, "Unable to connect to database: %v\n", err)
	}
	return conn, err
}

// Get the DB connection
func GetDB() (DB, error) {
	// we could use this method to ask for a local connection or put logic here to add a connection pooler in
	// a distributed system
	once.Do(func() {
		db.Session, dbError = postgresqlConnConfig()
	})

	return db, dbError
}
