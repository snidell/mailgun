package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v4"
	"github.com/mailgun/service/models"
	"net/http"
	"os"
)

const (
	DomainThreshold  = 1000
	BouncedThreshold = 1
)

type BaseHandler struct {
	db *pgx.Conn
}

func NewBaseHandler(db *pgx.Conn) *BaseHandler {
	return &BaseHandler{
		db: db,
	}
}

func (h *BaseHandler) DeliveredHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		errResp := false
		domainName := getDomain(r)
		resCode := http.StatusOK

		err := h.updateOrCreate(domainName, 1, 0)
		if err != nil {
			fmt.Println("Error occurred", err)
			errResp = true
		}

		if errResp == true {
			resCode = http.StatusInternalServerError
		}

		response := models.Response{
			Message:      "Successfully updated domain name",
			ResponseCode: resCode,
			Error:        errResp,
		}
		js, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, writeError := w.Write(js)

		if writeError != nil {
			fmt.Println("A write error occurred in DeliveredHandler", writeError)
		}
	}
}

func (h *BaseHandler) BouncedHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		errResp := false
		domainName := getDomain(r)

		resCode := http.StatusOK

		err := h.updateOrCreate(domainName, 0, 1)
		if err != nil {
			errResp = true
			fmt.Println("Error occurred in BounceHandler", err)
		}

		if errResp == true {
			resCode = http.StatusInternalServerError
		}

		response := models.Response{
			Message:      "Successfully updated domain name",
			ResponseCode: resCode,
			Error:        errResp,
		}
		js, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, writeError := w.Write(js)

		if writeError != nil {
			fmt.Println("A write error occurred in BouncedHandler", writeError)
		}
	}

}

func (h *BaseHandler) GetDomainHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		errResp := false
		domainName := getDomain(r)

		resCode := http.StatusOK

		event, err := h.get(domainName)
		if err != nil {
			errResp = true
			fmt.Println("Error occurred in GetDomainHandler", err)
		}
		domainType := determineDomain(event)

		if errResp == true {
			resCode = http.StatusInternalServerError
		}

		response := models.GetResponse{
			Response: models.Response{
				Message:      "Successfully updated domain name",
				ResponseCode: resCode,
				Error:        errResp,
			},
			Event:      event,
			DomainType: domainType,
		}

		js, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, writeError := w.Write(js)

		if writeError != nil {
			fmt.Println("A write error occurred in GetDomainHandler", writeError)
		}
	}
}

func getDomain(r *http.Request) string {
	// get URL parameters
	vars := mux.Vars(r)
	// find the metric name
	domainName := vars["domainName"]

	fmt.Println("msg", "Domain name received", "name", domainName)

	return domainName
}

func determineDomain(event models.Event) string {
	domainType := "unknown"
	//receives more than 1000 “delivered” events
	if event.Delivered > DomainThreshold {
		domainType = "catch-all"
	}
	//A domain name is not a catch-all when it receives at least 1 “bounced” event
	if event.Bounced >= BouncedThreshold {
		domainType = "not catch-all"
	}
	return domainType
}

// These could be abstracted into there own DB layer
func (h *BaseHandler) updateOrCreate(domain string, deliveredIncrease int64, bouncedIncrease int64) error {

	var exists bool
	var delivered int64
	var bounced int64

	err := h.db.QueryRow(context.Background(), "select exists(select 1 from events where domain=$1)", domain).Scan(&exists)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "QueryRow exists failed: %v\n", err)
		return err
	}
	//does the entry exist?
	if exists {
		err = h.db.QueryRow(context.Background(), "select domain, delivered, bounced from events where domain=$1", domain).Scan(&domain, &delivered, &bounced)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "select row failed: %v\n", err)
			return err
		}
		delivered = delivered + deliveredIncrease
		bounced = bounced + bouncedIncrease
		// update entry
		_, err = h.db.Exec(context.Background(), "update events set delivered=$1, bounced=$2 where domain=$3", delivered, bounced, domain)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "update row failed: %v\n", err)
			return err
		}
	} else {
		//create entry
		_, err = h.db.Exec(context.Background(), "insert into events(domain,delivered,bounced) values($1,$2,$3)", domain, deliveredIncrease, bouncedIncrease)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "create row failed: %v\n", err)
			return err
		}
	}

	return err

}

func (h *BaseHandler) get(domain string) (models.Event, error) {
	event := models.Event{
		Domain: domain,
	}
	err := h.db.QueryRow(context.Background(), "select domain, delivered, bounced from events where domain=$1", event.Domain).Scan(&event.Domain, &event.Delivered, &event.Bounced)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "QueryRow1 failed: %v\n", err)
	}

	return event, err
}
