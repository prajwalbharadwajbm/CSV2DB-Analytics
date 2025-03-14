package models

import (
	"time"
)

type Customer struct {
	CustomerID string `json:"customer_id"`
	Name       string `json:"name"`
	Email      string `json:"email"`
	Address    string `json:"address"`
}

type Product struct {
	ProductID string `json:"product_id"`
	Name      string `json:"name"`
	Category  string `json:"category"`
}

type Region struct {
	RegionID int    `json:"region_id"`
	Name     string `json:"name"`
}

type PaymentMethod struct {
	PaymentMethodID int    `json:"payment_method_id"`
	Name            string `json:"name"`
}

type Order struct {
	OrderID         string    `json:"order_id"`
	CustomerID      string    `json:"customer_id"`
	RegionID        int       `json:"region_id"`
	SaleDate        time.Time `json:"sale_date"`
	ShippingCost    float64   `json:"shipping_cost"`
	PaymentMethodID int       `json:"payment_method_id"`
}

type OrderItem struct {
	OrderItemID int     `json:"order_item_id"`
	OrderID     string  `json:"order_id"`
	ProductID   string  `json:"product_id"`
	Quantity    int     `json:"quantity"`
	UnitPrice   float64 `json:"unit_price"`
	Discount    float64 `json:"discount"`
}

type DataRefreshLog struct {
	LogID         int        `json:"log_id"`
	StartTime     time.Time  `json:"start_time"`
	EndTime       *time.Time `json:"end_time"`
	Status        string     `json:"status"`
	RowsProcessed int        `json:"rows_processed"`
	ErrorMessage  *string    `json:"error_message"`
	TriggeredBy   string     `json:"triggered_by"`
}

type Filter struct {
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
	Category  string `json:"category,omitempty"`
	Region    string `json:"region,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}
