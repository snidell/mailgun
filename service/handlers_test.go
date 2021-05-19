package main

import (
	"github.com/jackc/pgx/v4"
	"github.com/mailgun/service/models"
	"github.com/mailgun/service/repo"
	"testing"
)

// other test cases can be made here to increase cover
func TestBaseHandler_get(t *testing.T) {
	type fields struct {
		db *pgx.Conn
	}
	type args struct {
		domain string
	}
	type test struct {
		name    string
		fields  fields
		args    args
		want    models.Event
		wantErr bool
	}

	dbConn, dbError := repo.GetDB()
	if dbError != nil {
		t.Errorf("repo.GetDB() error = %v", dbError)
	}
	field1 := fields{db: dbConn.Session}
	args1 := args{
		domain: "google.com",
	}

	event := models.Event{
		Domain:    "google.com",
		Delivered: 100,
		Bounced:   25,
	}

	var tests []test
	test1 := test{
		name:    "Test1",
		fields:  field1,
		args:    args1,
		want:    event,
		wantErr: true,
	}
	tests = append(tests, test1)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &BaseHandler{
				db: tt.fields.db,
			}
			got, err := h.get(tt.args.domain)
			if err != nil {
				t.Errorf("get() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !(got.Bounced < tt.want.Bounced) {
				t.Errorf("get() got = %v, want %v", got.Bounced, tt.want.Bounced)
			}
			if !(got.Delivered < tt.want.Delivered) {
				t.Errorf("get() got = %v, want %v", got.Delivered, tt.want.Delivered)
			}
		})
	}
}

func TestBaseHandler_updateOrCreate(t *testing.T) {
	type fields struct {
		db *pgx.Conn
	}
	type args struct {
		domain            string
		deliveredIncrease int64
		bouncedIncrease   int64
	}
	type test struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}

	dbConn, dbError := repo.GetDB()
	if dbError != nil {
		t.Errorf("repo.GetDB() error = %v", dbError)
	}
	field1 := fields{db: dbConn.Session}
	args1 := args{
		domain:            "google.com",
		deliveredIncrease: 0,
		bouncedIncrease:   1,
	}

	var tests []test
	test1 := test{
		name:    "Test1",
		fields:  field1,
		args:    args1,
		wantErr: true,
	}
	tests = append(tests, test1)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &BaseHandler{
				db: tt.fields.db,
			}
			if err := h.updateOrCreate(tt.args.domain, tt.args.deliveredIncrease, tt.args.bouncedIncrease); (err != nil) != tt.wantErr {
				if err != nil {
					t.Errorf("updateOrCreate() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}

func Test_determineDomain(t *testing.T) {
	type args struct {
		event models.Event
	}
	type test struct {
		name string
		args args
		want string
	}

	args1 := args{
		models.Event{
			Domain:    "google.com",
			Delivered: 1,
			Bounced:   0,
		},
	}
	args2 := args{
		models.Event{
			Domain:    "google.com",
			Delivered: 10001,
			Bounced:   0,
		},
	}
	args3 := args{
		models.Event{
			Domain:    "google.com",
			Delivered: 1,
			Bounced:   1,
		},
	}

	var tests []test

	test1 := test{
		name: "Test Unknown",
		args: args1,
		want: "unknown",
	}
	test2 := test{
		name: "Test Catch-all",
		args: args2,
		want: "catch-all",
	}
	test3 := test{
		name: "Test Not Catch-all",
		args: args3,
		want: "not catch-all",
	}

	tests = append(tests, test1, test2, test3)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := determineDomain(tt.args.event); got != tt.want {
				t.Errorf("determineDomain() = %v, want %v", got, tt.want)
			}
		})
	}
}
