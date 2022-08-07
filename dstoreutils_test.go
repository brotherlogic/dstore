package main

import "testing"

func TestBasic(t *testing.T) {
	if true {
		t.Errorf("Bad test")
	}
}
