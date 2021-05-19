package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/mailgun/service/repo"
	"net/http"
	"time"
)

var (
	port   string
	dbConn *repo.DB
)

func main() {
	parseArguements()

	dbConn, dbError := repo.GetDB()
	defer dbConn.Session.Close(context.Background())
	if dbError != nil {
		fmt.Println("msg", "Service failed to obtain database connection", "err", dbError)
		panic("Service failed to obtain database connection")
	}
	fmt.Println("msg", "Get database connection complete.")

	h := NewBaseHandler(dbConn.Session)

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/events/{domainName}/delivered", h.DeliveredHandler()).Methods("PUT")
	router.HandleFunc("/events/{domainName}/bounced", h.BouncedHandler()).Methods("PUT")
	router.HandleFunc("/domains/{domainName}", h.GetDomainHandler()).Methods("GET")

	server := &http.Server{
		Addr:        ":" + port,
		Handler:     router,
		ReadTimeout: 500 * time.Millisecond,
		IdleTimeout: 90 * time.Second,
	}

	// here we could determine to use TLS and certs if needed to look something like this
	//if enableTLS {
	//	fmt.Println("msg", "Starting Service on TLS", "port", port)
	//	if err := server.ListenAndServeTLS(certs.CertBasePath+certs.CertPath, certs.CertBasePath+certs.CertKeyPath); err != nil {
	//		fmt.Println("msg", "Cannot start Service ListenAndServe failed", "err", err.Error())
	//		panic("Cannot start Service ListenAndServe failed")
	//	}
	//}

	if err := server.ListenAndServe(); err != nil {
		fmt.Println("msg", "Cannot start Service ListenAndServe failed", "err", err.Error())
		panic("Cannot start Service ListenAndServe failed")
	}
}

func parseArguements() {
	flag.StringVar(&port, "port", "1337", "port to run on")
	flag.Parse()

	fmt.Println("port:", port)
}
