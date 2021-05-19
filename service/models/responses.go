package models

type Response struct{
	Message string  `json:"message"`
	ResponseCode int    `json:"code"`
	Error        bool   `json:"error"`
}

type GetResponse struct{
	Response Response
	Event Event
	DomainType string `json:"domain_type"`
}




