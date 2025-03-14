package services

import (
	"context"
	"errors"

	"github.com/prajwalbharadwajbm/backend_assessment/internal/database"
)

type AnalyticsService struct {
	db *database.DB
}

func NewAnalyticsService(db *database.DB) *AnalyticsService {
	return &AnalyticsService{db: db}
}

// GetRevenueByDateRange calculates total revenue within the given date range
func (as *AnalyticsService) GetRevenueByDateRange(ctx context.Context, startDate, endDate string) (float64, error) {
	query := `
		SELECT COALESCE(SUM((oi.unit_price * oi.quantity) * (1 - oi.discount)), 0) as total_revenue
		FROM order_items oi
		JOIN orders o ON oi.order_id = o.order_id
		WHERE o.sale_date BETWEEN $1 AND $2
	`

	var totalRevenue float64
	err := as.db.QueryRowContext(ctx, query, startDate, endDate).Scan(&totalRevenue)
	if err != nil {
		return 0, err
	}

	return totalRevenue, nil
}

// GetRevenueByProduct calculates revenue for each product within the given date range
func (as *AnalyticsService) GetRevenueByProduct(ctx context.Context, startDate, endDate string) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			p.product_id,
			p.name,
			COALESCE(SUM((oi.unit_price * oi.quantity) * (1 - oi.discount)), 0) as revenue
		FROM products p
		JOIN order_items oi ON p.product_id = oi.product_id
		JOIN orders o ON oi.order_id = o.order_id
		WHERE o.sale_date BETWEEN $1 AND $2
		GROUP BY p.product_id, p.name
		ORDER BY revenue DESC
	`

	rows, err := as.db.QueryContext(ctx, query, startDate, endDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}

	// Iterating over the rows and appending the results to the results slice
	for rows.Next() {
		var productID, name string
		var revenue float64

		if err := rows.Scan(&productID, &name, &revenue); err != nil {
			return nil, err
		}

		results = append(results, map[string]interface{}{
			"product_id": productID,
			"name":       name,
			"revenue":    revenue,
		})
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// GetRevenueByCategory calculates revenue for each category within the given date range
func (as *AnalyticsService) GetRevenueByCategory(ctx context.Context, startDate, endDate string) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			p.category,
			COALESCE(SUM((oi.unit_price * oi.quantity) * (1 - oi.discount)), 0) as revenue
		FROM products p
		JOIN order_items oi ON p.product_id = oi.product_id
		JOIN orders o ON oi.order_id = o.order_id
		WHERE o.sale_date BETWEEN $1 AND $2
		GROUP BY p.category
		ORDER BY revenue DESC
	`

	rows, err := as.db.QueryContext(ctx, query, startDate, endDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}

	for rows.Next() {
		var category string
		var revenue float64

		if err := rows.Scan(&category, &revenue); err != nil {
			return nil, err
		}

		results = append(results, map[string]interface{}{
			"category": category,
			"revenue":  revenue,
		})
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// GetRevenueByRegion calculates revenue for each region within the given date range
func (as *AnalyticsService) GetRevenueByRegion(ctx context.Context, startDate, endDate string) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			r.name as region,
			COALESCE(SUM((oi.unit_price * oi.quantity) * (1 - oi.discount)), 0) as revenue
		FROM regions r
		JOIN orders o ON r.region_id = o.region_id
		JOIN order_items oi ON o.order_id = oi.order_id
		WHERE o.sale_date BETWEEN $1 AND $2
		GROUP BY r.name
		ORDER BY revenue DESC
	`

	rows, err := as.db.QueryContext(ctx, query, startDate, endDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}

	for rows.Next() {
		var region string
		var revenue float64

		if err := rows.Scan(&region, &revenue); err != nil {
			return nil, err
		}

		results = append(results, map[string]interface{}{
			"region":  region,
			"revenue": revenue,
		})
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// GetRevenueOverTime calculates revenue trends over time within the given date range
func (as *AnalyticsService) GetRevenueOverTime(ctx context.Context, startDate, endDate, interval string) ([]map[string]interface{}, error) {
	var timeFormat string
	var groupBy string

	switch interval {
	case "monthly":
		timeFormat = "YYYY-MM"
		groupBy = "DATE_TRUNC('month', o.sale_date)"
	case "quarterly":
		timeFormat = "YYYY-\"Q\"Q"
		groupBy = "DATE_TRUNC('quarter', o.sale_date)"
	case "yearly":
		timeFormat = "YYYY"
		groupBy = "DATE_TRUNC('year', o.sale_date)"
	default:
		return nil, errors.New("invalid interval: must be 'monthly', 'quarterly', or 'yearly'")
	}

	query := `
		SELECT 
			TO_CHAR(` + groupBy + `, '` + timeFormat + `') as time_period,
			COALESCE(SUM((oi.unit_price * oi.quantity) * (1 - oi.discount)), 0) as revenue
		FROM orders o
		JOIN order_items oi ON o.order_id = oi.order_id
		WHERE o.sale_date BETWEEN $1 AND $2
		GROUP BY time_period
		ORDER BY time_period
	`

	rows, err := as.db.QueryContext(ctx, query, startDate, endDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}

	for rows.Next() {
		var timePeriod string
		var revenue float64

		if err := rows.Scan(&timePeriod, &revenue); err != nil {
			return nil, err
		}

		results = append(results, map[string]interface{}{
			"time_period": timePeriod,
			"revenue":     revenue,
		})
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}
