// SPDX-FileCopyrightText: 2026 2M Production Electrique
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import "testing"

func TestSelectOutboundByUser(t *testing.T) {
	a100 := &sipAccount{Extension: "100", NextcloudUser: "alexandre", CallerID: "+331", Default: true}
	a101 := &sipAccount{Extension: "101", NextcloudUser: "paul", CallerID: "+331"}
	m := &accountManager{byExtension: map[string]*sipAccount{"100": a100, "101": a101}, byUser: map[string]*sipAccount{"alexandre": a100, "paul": a101}, accounts: []*sipAccount{a100, a101}, def: a100}
	got, err := m.SelectOutbound("+331", "paul")
	if err != nil {
		t.Fatal(err)
	}
	if got.Extension != "101" {
		t.Fatalf("got %s, want 101", got.Extension)
	}
}

func TestSelectOutboundDefault(t *testing.T) {
	a100 := &sipAccount{Extension: "100", NextcloudUser: "alexandre", Default: true}
	m := &accountManager{byExtension: map[string]*sipAccount{"100": a100}, byUser: map[string]*sipAccount{"alexandre": a100}, accounts: []*sipAccount{a100}, def: a100}
	got, err := m.SelectOutbound("", "unknown")
	if err != nil {
		t.Fatal(err)
	}
	if got.Extension != "100" {
		t.Fatalf("got %s, want 100", got.Extension)
	}
}
