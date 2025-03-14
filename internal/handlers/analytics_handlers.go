package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/prajwalbharadwajbm/backend_assessment/internal/services"
)

type AnalyticsHandler struct {
	analyticsService *services.AnalyticsService
	dataLoader       *services.DataLoader
}

func NewAnalyticsHandler(as *services.AnalyticsService, dl *services.DataLoader) *AnalyticsHandler {
	return &AnalyticsHandler{
		analyticsService: as,
		dataLoader:       dl,
	}
}

// validateDateRange checks if the date range is valid
func validateDateRange(startDate, endDate string) (string, string, error) {
	if startDate == "" {
		startDate = time.Now().AddDate(-1, 0, 0).Format("2006-01-02") // Default to 1 year ago
	}

	if endDate == "" {
		endDate = time.Now().Format("2006-01-02") // Default to today
	}

	// Additional validation could be added here

	return startDate, endDate, nil
}

// RespondWithJSON helper function to respond with JSON
func RespondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "Failed to marshal JSON response"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

// RespondWithError helper function to respond with an error
func RespondWithError(w http.ResponseWriter, code int, message string) {
	RespondWithJSON(w, code, map[string]string{"error": message})
}

// GetTotalRevenue handles requests for total revenue
func (h *AnalyticsHandler) GetTotalRevenue(w http.ResponseWriter, r *http.Request) {
	startDate := r.URL.Query().Get("start_date")
	endDate := r.URL.Query().Get("end_date")

	var err error
	startDate, endDate, err = validateDateRange(startDate, endDate)
	if err != nil {
		RespondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	revenue, err := h.analyticsService.GetRevenueByDateRange(r.Context(), startDate, endDate)
	if err != nil {
		RespondWithError(w, http.StatusInternalServerError, "Failed to calculate revenue: "+err.Error())
		return
	}

	RespondWithJSON(w, http.StatusOK, map[string]interface{}{
		"start_date": startDate,
		"end_date":   endDate,
		"revenue":    revenue,
	})
}

// GetRevenueByProduct handles requests for revenue by product
func (h *AnalyticsHandler) GetRevenueByProduct(w http.ResponseWriter, r *http.Request) {
	startDate := r.URL.Query().Get("start_date")
	endDate := r.URL.Query().Get("end_date")

	var err error
	startDate, endDate, err = validateDateRange(startDate, endDate)
	if err != nil {
		RespondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	revenues, err := h.analyticsService.GetRevenueByProduct(r.Context(), startDate, endDate)
	if err != nil {
		RespondWithError(w, http.StatusInternalServerError, "Failed to calculate revenue by product: "+err.Error())
		return
	}

	RespondWithJSON(w, http.StatusOK, map[string]interface{}{
		"start_date": startDate,
		"end_date":   endDate,
		"data":       revenues,
	})
}

// GetRevenueByCategory handles requests for revenue by category
func (h *AnalyticsHandler) GetRevenueByCategory(w http.ResponseWriter, r *http.Request) {
	startDate := r.URL.Query().Get("start_date")
	endDate := r.URL.Query().Get("end_date")

	var err error
	startDate, endDate, err = validateDateRange(startDate, endDate)
	if err != nil {
		RespondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	revenues, err := h.analyticsService.GetRevenueByCategory(r.Context(), startDate, endDate)
	if err != nil {
		RespondWithError(w, http.StatusInternalServerError, "Failed to calculate revenue by category: "+err.Error())
		return
	}

	RespondWithJSON(w, http.StatusOK, map[string]interface{}{
		"start_date": startDate,
		"end_date":   endDate,
		"data":       revenues,
	})
}

// GetRevenueByRegion handles requests for revenue by region
func (h *AnalyticsHandler) GetRevenueByRegion(w http.ResponseWriter, r *http.Request) {
	startDate := r.URL.Query().Get("start_date")
	endDate := r.URL.Query().Get("end_date")

	var err error
	startDate, endDate, err = validateDateRange(startDate, endDate)
	if err != nil {
		RespondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	revenues, err := h.analyticsService.GetRevenueByRegion(r.Context(), startDate, endDate)
	if err != nil {
		RespondWithError(w, http.StatusInternalServerError, "Failed to calculate revenue by region: "+err.Error())
		return
	}

	RespondWithJSON(w, http.StatusOK, map[string]interface{}{
		"start_date": startDate,
		"end_date":   endDate,
		"data":       revenues,
	})
}

// GetRevenueOverTime handles requests for revenue trends over time
func (h *AnalyticsHandler) GetRevenueOverTime(w http.ResponseWriter, r *http.Request) {
	startDate := r.URL.Query().Get("start_date")
	endDate := r.URL.Query().Get("end_date")
	interval := r.URL.Query().Get("interval")

	if interval == "" {
		interval = "monthly"
	}

	var err error
	startDate, endDate, err = validateDateRange(startDate, endDate)
	if err != nil {
		RespondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	revenues, err := h.analyticsService.GetRevenueOverTime(r.Context(), startDate, endDate, interval)
	if err != nil {
		RespondWithError(w, http.StatusInternalServerError, "Failed to calculate revenue over time: "+err.Error())
		return
	}

	RespondWithJSON(w, http.StatusOK, map[string]interface{}{
		"start_date": startDate,
		"end_date":   endDate,
		"interval":   interval,
		"data":       revenues,
	})
}

// TriggerDataRefresh handles requests to manually trigger a data refresh
func (h *AnalyticsHandler) TriggerDataRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		RespondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var requestBody struct {
		FilePath string `json:"file_path"`
	}

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&requestBody); err != nil {
		RespondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if requestBody.FilePath == "" {
		RespondWithError(w, http.StatusBadRequest, "File path is required")
		return
	}

	// Start the refresh in a goroutine so it doesn't block the response
	go func() {
		if err := h.dataLoader.RefreshData(context.Background(), requestBody.FilePath, "API"); err != nil {
			// Log the error but don't return it since we're in a goroutine
			log.Printf("Data refresh failed: %v", err)
		}
	}()

	RespondWithJSON(w, http.StatusAccepted, map[string]string{
		"message": "Data refresh triggered successfully",
	})
}
