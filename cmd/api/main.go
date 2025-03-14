package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prajwalbharadwajbm/backend_assessment/config"
	"github.com/prajwalbharadwajbm/backend_assessment/internal/database"
	"github.com/prajwalbharadwajbm/backend_assessment/internal/handlers"
	"github.com/prajwalbharadwajbm/backend_assessment/internal/services"
)

func main() {
	logger := setupLogger()

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatalf("Failed to load configuration: %v", err)
	}

	db, err := database.New(cfg)
	if err != nil {
		logger.Fatalf("Failed to connect to database: %v", err)
	}
	// Closing the database connection when the main function is finished
	// avoid connection leaks
	defer db.Close()

	// Initializing services package which has all the business logic
	// dataLoader is responsible for loading the data from the csv file, can be scheduled refresh or triggered by API
	dataLoader := services.NewDataLoader(db, cfg, logger)

	// analyticsService is responsible for providing the analytics data as provided in the problem statement
	analyticsService := services.NewAnalyticsService(db)

	// Initialize context for background refresh scheduler
	ctx, cancel := context.WithCancel(context.Background())
	// cancel the context when the main function is finished
	defer cancel()

	refreshScheduler := services.NewRefreshScheduler(dataLoader, cfg, logger)
	if err := refreshScheduler.Start(ctx, cfg.RefreshSchedule); err != nil {
		logger.Printf("Warning: Failed to start refresh scheduler: %v", err)
	}
	defer refreshScheduler.Stop()

	analyticsHandler := handlers.NewAnalyticsHandler(analyticsService, dataLoader)

	router := setupRoutes(analyticsHandler)

	server := &http.Server{
		Addr:         ":" + cfg.ServerPort,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine so it doesn't block
	logger.Printf("Starting server on port %s", cfg.ServerPort)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatalf("Failed to start server: %v", err)
	}

	// Create a deadline to wait for current operations to complete
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Fatalf("Server shutdown failed: %v", err)
	}

	logger.Println("Server gracefully stopped")
}

// helper function to setup logger
func setupLogger() *log.Logger {
	logFile, err := os.OpenFile("logs/application.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	return log.New(logFile, "SALES-ANALYTICS: ", log.LstdFlags)
}
