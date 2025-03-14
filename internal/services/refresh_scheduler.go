package services

import (
	"context"
	"log"
	"time"

	"github.com/prajwalbharadwajbm/backend_assessment/config"
	"github.com/robfig/cron/v3"
)

type RefreshScheduler struct {
	dataLoader *DataLoader
	cron       *cron.Cron
	logger     *log.Logger
	csvPath    string
}

func NewRefreshScheduler(dataLoader *DataLoader, cfg *config.Config, logger *log.Logger) *RefreshScheduler {
	return &RefreshScheduler{
		dataLoader: dataLoader,
		cron:       cron.New(),
		logger:     logger,
		csvPath:    cfg.DefaultCSVPath, // Default CSV path fetched from env variable(can be overridden via API in payload)
	}
}

// Start begins the scheduler
func (rs *RefreshScheduler) Start(ctx context.Context, schedule string) error {
	rs.logger.Printf("Starting data refresh scheduler with schedule: %s", schedule)

	_, err := rs.cron.AddFunc(schedule, func() {
		rs.logger.Println("Running scheduled data refresh")

		// Using 1 hour timeout for the refresh operation,
		// we can optimize it based on time it takes to refresh the data
		// that depends on the size of the data in the csv file
		refreshCtx, cancel := context.WithTimeout(ctx, 1*time.Hour)
		defer cancel()

		if err := rs.dataLoader.RefreshData(refreshCtx, rs.csvPath, "SCHEDULER"); err != nil {
			rs.logger.Printf("Scheduled data refresh failed: %v", err)
		} else {
			rs.logger.Println("Scheduled data refresh completed successfully")
		}
	})

	if err != nil {
		return err
	}

	rs.cron.Start()
	return nil
}

// To Stop the scheduler
func (rs *RefreshScheduler) Stop() {
	rs.cron.Stop()
}
