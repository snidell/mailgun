package models

type Event struct {
	Domain            string       `json:"domain_name"`
	Delivered    int       `json:"delivered"`
	Bounced          int       `json:"bounced"`
}