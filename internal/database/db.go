package database

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/prajwalbharadwajbm/backend_assessment/config"
)

// I have set max open connections and idle connections considering development purposes.
const (
	maxOpenConns = 25
	maxIdleConns = 5
)

type DB struct {
	*sql.DB
}

// Constructor function to create a new database connection.
func New(cfg *config.Config) (*DB, error) {
	// Using lib/pq for conencting to postgres db
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName, cfg.DBSSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Open Connection doesn't validate the connection is open or not
	// so we need to ping the database to check if the connection is open
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// I am using postgres on mac, where i am configured to use 100 connections by default.
	// Considering scaleability, we need to ensure to set max open connections and idle connections based on our use case.
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)

	return &DB{db}, nil
}
