package main

import (
	"github.com/gorilla/mux"
	"github.com/prajwalbharadwajbm/backend_assessment/internal/handlers"
)

// setupRoutes configures and handles all the routes
// Using mux, if more time given, could have added middleware request validation, jwt auth etc and other routes
func setupRoutes(analyticsHandler *handlers.AnalyticsHandler) *mux.Router {
	router := mux.NewRouter()

	// Analytics endpoints
	router.HandleFunc("/api/revenue/total", analyticsHandler.GetTotalRevenue).Methods("GET")
	router.HandleFunc("/api/revenue/by-product", analyticsHandler.GetRevenueByProduct).Methods("GET")
	router.HandleFunc("/api/revenue/by-category", analyticsHandler.GetRevenueByCategory).Methods("GET")
	router.HandleFunc("/api/revenue/by-region", analyticsHandler.GetRevenueByRegion).Methods("GET")
	router.HandleFunc("/api/revenue/over-time", analyticsHandler.GetRevenueOverTime).Methods("GET")

	// Data refresh endpoint
	router.HandleFunc("/api/data/refresh", analyticsHandler.TriggerDataRefresh).Methods("POST")

	return router
}
