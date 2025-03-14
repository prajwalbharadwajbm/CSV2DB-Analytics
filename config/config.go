package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	DBHost           string
	DBPort           string
	DBUser           string
	DBPassword       string
	DBName           string
	DBSSLMode        string
	ServerPort       string
	RefreshSchedule  string
	LogPath          string
	RefreshBatchSize int
	DefaultCSVPath   string
}

// LoadConfig loads the configuration for the application using godotenv package
// This avoids hardcoding values and also when deployed in cloud, we can use azure env variables
// I am using godotenv to load the environment variables from .env
// This avoids hardcoding values and also when deployed in cloud, we can use azure env variables
func LoadConfig() (*Config, error) {
	err := godotenv.Load()
	if err != nil {
		return nil, fmt.Errorf("error loading .env file: %w", err)
	}
	godotenv.Load()

	batchSize, _ := strconv.Atoi(getEnv("REFRESH_BATCH_SIZE", "1000"))

	return &Config{
		DBHost:           getEnv("DB_HOST", "localhost"),
		DBPort:           getEnv("DB_PORT", "5432"),
		DBUser:           getEnv("DB_USER", "postgres"),
		DBPassword:       getEnv("DB_PASSWORD", "postgres"),
		DBName:           getEnv("DB_NAME", "sales_analytics"),
		DBSSLMode:        getEnv("DB_SSL_MODE", "disable"),
		ServerPort:       getEnv("SERVER_PORT", "8080"),
		RefreshSchedule:  getEnv("REFRESH_SCHEDULE", "0 0 * * *"), // By Deafult i will be running daily at midnight
		LogPath:          getEnv("LOG_PATH", "logs/application.log"),
		RefreshBatchSize: batchSize,
		DefaultCSVPath:   getEnv("DEFAULT_CSV_PATH", "./sample.csv"),
	}, nil
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
