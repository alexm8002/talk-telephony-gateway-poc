// SPDX-FileCopyrightText: 2026 2M Production Electrique
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFetchAccountsAPI(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":1,"accounts":[{"extension":"100","username":"100","auth_user":"100","password":"secret","caller_id":"+331","nextcloud_user":"alexandre","default":true}]}`))
	}))
	defer srv.Close()

	m := &accountManager{
		cfg:       config{AccountsAPIURL: srv.URL, GatewayAPIToken: "abc"},
		apiClient: &http.Client{Timeout: time.Second},
	}
	got, err := m.fetchAccounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer abc" {
		t.Fatalf("Authorization=%q", gotAuth)
	}
	if len(got.Accounts) != 1 || got.Accounts[0].Extension != "100" {
		t.Fatalf("unexpected payload: %#v", got)
	}
}

func TestNormalizeAccountsRejectsDuplicateUser(t *testing.T) {
	_, _, _, _, err := normalizeAccounts([]sipAccount{
		{Extension: "100", Password: "a", NextcloudUser: "same"},
		{Extension: "200", Password: "b", NextcloudUser: "same"},
	})
	if err == nil {
		t.Fatal("expected duplicate nextcloud_user error")
	}
}
