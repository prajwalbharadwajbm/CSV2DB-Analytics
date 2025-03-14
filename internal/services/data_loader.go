package services

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/prajwalbharadwajbm/backend_assessment/config"
	"github.com/prajwalbharadwajbm/backend_assessment/internal/database"
)

type DataLoader struct {
	db     *database.DB
	config *config.Config
	logger *log.Logger
}

func NewDataLoader(db *database.DB, cfg *config.Config, logger *log.Logger) *DataLoader {
	return &DataLoader{
		db:     db,
		config: cfg,
		logger: logger,
	}
}

// StartDataRefresh begins a data refresh process and logs it
// triggeredBy is the source of the refresh, can be API or scheduled refresh
func (dl *DataLoader) StartDataRefresh(ctx context.Context, triggeredBy string) (int, error) {
	// Start a refresh log
	var logID int
	err := dl.db.QueryRowContext(
		ctx,
		`INSERT INTO data_refresh_logs (start_time, status, triggered_by) 
         VALUES ($1, $2, $3) RETURNING log_id`,
		time.Now(),
		"STARTED",
		triggeredBy,
	).Scan(&logID)

	if err != nil {
		return 0, fmt.Errorf("failed to create refresh log: %w", err)
	}

	return logID, nil
}

// CompleteDataRefresh updates the refresh log with status
func (dl *DataLoader) CompleteDataRefresh(ctx context.Context, logID int, rowsProcessed int, err error) error {
	status := "COMPLETED"
	var errMsg *string

	if err != nil {
		status = "FAILED"
		msg := err.Error()
		errMsg = &msg
	}

	_, updateErr := dl.db.ExecContext(
		ctx,
		`UPDATE data_refresh_logs 
         SET end_time = $1, status = $2, rows_processed = $3, error_message = $4
         WHERE log_id = $5`,
		time.Now(),
		status,
		rowsProcessed,
		errMsg,
		logID,
	)

	if updateErr != nil {
		return fmt.Errorf("failed to update refresh log: %w", updateErr)
	}

	return nil
}

func (dl *DataLoader) RefreshData(ctx context.Context, filePath string, triggeredBy string) error {
	logID, err := dl.StartDataRefresh(ctx, triggeredBy)
	if err != nil {
		return err
	}

	rowsProcessed := 0

	file, err := os.Open(filePath)
	if err != nil {
		dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	header, err := reader.Read()
	if err != nil {
		dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	columnMap := make(map[string]int)
	for i, col := range header {
		columnMap[col] = i
	}

	tx, err := dl.db.BeginTx(ctx, nil)
	if err != nil {
		dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	insertCustomerStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO customers (customer_id, name, email, address)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (customer_id) DO UPDATE
		SET name = $2, email = $3, address = $4
	`)
	if err != nil {
		tx.Rollback()
		dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
		return fmt.Errorf("failed to prepare customer statement: %w", err)
	}
	defer insertCustomerStmt.Close()

	insertProductStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO products (product_id, name, category)
		VALUES ($1, $2, $3)
		ON CONFLICT (product_id) DO UPDATE
		SET name = $2, category = $3
	`)
	if err != nil {
		tx.Rollback()
		dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
		return fmt.Errorf("failed to prepare product statement: %w", err)
	}
	defer insertProductStmt.Close()

	insertRegionStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO regions (name)
		VALUES ($1)
		ON CONFLICT (name) DO NOTHING
	`)
	if err != nil {
		tx.Rollback()
		dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
		return fmt.Errorf("failed to prepare region statement: %w", err)
	}
	defer insertRegionStmt.Close()

	insertPaymentMethodStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO payment_methods (name)
		VALUES ($1)
		ON CONFLICT (name) DO NOTHING
	`)
	if err != nil {
		tx.Rollback()
		dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
		return fmt.Errorf("failed to prepare payment method statement: %w", err)
	}
	defer insertPaymentMethodStmt.Close()

	insertOrderStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO orders (order_id, customer_id, region_id, sale_date, shipping_cost, payment_method_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (order_id) DO NOTHING
	`)
	if err != nil {
		tx.Rollback()
		dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
		return fmt.Errorf("failed to prepare order statement: %w", err)
	}
	defer insertOrderStmt.Close()

	insertOrderItemStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		tx.Rollback()
		dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
		return fmt.Errorf("failed to prepare order item statement: %w", err)
	}
	defer insertOrderItemStmt.Close()

	batch := 0
	batchSize := dl.config.RefreshBatchSize

	for {
		batch++
		batchRowsProcessed := 0

		for i := 0; i < batchSize; i++ {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				tx.Rollback()
				dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
				return fmt.Errorf("error reading CSV record: %w", err)
			}

			err = dl.processRecord(ctx, tx, record, columnMap, insertCustomerStmt, insertProductStmt, insertOrderStmt, insertOrderItemStmt)
			if err != nil {
				tx.Rollback()
				dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
				return fmt.Errorf("error processing record: %w", err)
			}

			batchRowsProcessed++
			rowsProcessed++
		}

		if batchRowsProcessed == 0 {
			break
		}

		dl.logger.Printf("Processed batch %d (%d rows so far)", batch, rowsProcessed)
	}

	if err := tx.Commit(); err != nil {
		dl.CompleteDataRefresh(ctx, logID, rowsProcessed, err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return dl.CompleteDataRefresh(ctx, logID, rowsProcessed, nil)
}

// processRecord processes a single CSV record
func (dl *DataLoader) processRecord(
	ctx context.Context,
	tx *sql.Tx,
	record []string,
	columnMap map[string]int,
	customerStmt *sql.Stmt,
	productStmt *sql.Stmt,

	orderStmt *sql.Stmt,
	orderItemStmt *sql.Stmt,
) error {
	var (
		orderID         string
		productID       string
		customerID      string
		productName     string
		category        string
		region          string
		saleDate        time.Time
		quantitySold    int
		unitPrice       float64
		discount        float64
		shippingCost    float64
		paymentMethod   string
		customerName    string
		customerEmail   string
		customerAddress string
	)

	getColumnValue := func(colName string) (string, error) {
		idx, exists := columnMap[colName]
		if !exists {
			return "", fmt.Errorf("column %s not found in CSV", colName)
		}
		if idx >= len(record) {
			return "", fmt.Errorf("record has fewer columns than expected")
		}
		return record[idx], nil
	}

	var err error
	if orderID, err = getColumnValue("Order ID"); err != nil {
		return err
	}

	if productID, err = getColumnValue("Product ID"); err != nil {
		return err
	}

	if customerID, err = getColumnValue("Customer ID"); err != nil {
		return err
	}

	if productName, err = getColumnValue("Product Name"); err != nil {
		return err
	}

	if category, err = getColumnValue("Category"); err != nil {
		return err
	}

	if region, err = getColumnValue("Region"); err != nil {
		return err
	}

	saleDateStr, err := getColumnValue("Date of Sale")
	if err != nil {
		return err
	}
	saleDate, err = time.Parse("2006-01-02", saleDateStr)
	if err != nil {
		return fmt.Errorf("invalid date format: %w", err)
	}

	quantityStr, err := getColumnValue("Quantity Sold")
	if err != nil {
		return err
	}
	quantitySold, err = strconv.Atoi(quantityStr)
	if err != nil {
		return fmt.Errorf("invalid quantity: %w", err)
	}

	unitPriceStr, err := getColumnValue("Unit Price")
	if err != nil {
		return err
	}
	unitPrice, err = strconv.ParseFloat(unitPriceStr, 64)
	if err != nil {
		return fmt.Errorf("invalid unit price: %w", err)
	}

	discountStr, err := getColumnValue("Discount")
	if err != nil {
		return err
	}
	discount, err = strconv.ParseFloat(discountStr, 64)
	if err != nil {
		return fmt.Errorf("invalid discount: %w", err)
	}

	shippingCostStr, err := getColumnValue("Shipping Cost")
	if err != nil {
		return err
	}
	shippingCost, err = strconv.ParseFloat(shippingCostStr, 64)
	if err != nil {
		return fmt.Errorf("invalid shipping cost: %w", err)
	}

	if paymentMethod, err = getColumnValue("Payment Method"); err != nil {
		return err
	}

	if customerName, err = getColumnValue("Customer Name"); err != nil {
		return err
	}

	if customerEmail, err = getColumnValue("Customer Email"); err != nil {
		return err
	}

	if customerAddress, err = getColumnValue("Customer Address"); err != nil {
		return err
	}

	var regionID int
	err = tx.QueryRowContext(
		ctx,
		`INSERT INTO regions (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING region_id`,
		region,
	).Scan(&regionID)
	if err != nil {
		return fmt.Errorf("failed to insert/get region: %w", err)
	}

	var paymentMethodID int
	err = tx.QueryRowContext(
		ctx,
		`INSERT INTO payment_methods (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING payment_method_id`,
		paymentMethod,
	).Scan(&paymentMethodID)
	if err != nil {
		return fmt.Errorf("failed to insert/get payment method: %w", err)
	}

	_, err = customerStmt.ExecContext(
		ctx,
		customerID,
		customerName,
		customerEmail,
		customerAddress,
	)
	if err != nil {
		return fmt.Errorf("failed to insert customer: %w", err)
	}

	_, err = productStmt.ExecContext(
		ctx,
		productID,
		productName,
		category,
	)
	if err != nil {
		return fmt.Errorf("failed to insert product: %w", err)
	}

	var orderExists bool
	err = tx.QueryRowContext(
		ctx,
		"SELECT EXISTS(SELECT 1 FROM orders WHERE order_id = $1)",
		orderID,
	).Scan(&orderExists)
	if err != nil {
		return fmt.Errorf("failed to check if order exists: %w", err)
	}

	if !orderExists {
		_, err = orderStmt.ExecContext(
			ctx,
			orderID,
			customerID,
			regionID,
			saleDate,
			shippingCost,
			paymentMethodID,
		)
		if err != nil {
			return fmt.Errorf("failed to insert order: %w", err)
		}
	}

	_, err = orderItemStmt.ExecContext(
		ctx,
		orderID,
		productID,
		quantitySold,
		unitPrice,
		discount,
	)
	if err != nil {
		return fmt.Errorf("failed to insert order item: %w", err)
	}

	return nil
}
