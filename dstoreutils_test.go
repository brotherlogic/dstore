package main

import "testing"

func RunBasicTest(t *testing.T) {
	if true {
		t.Errorf("Bad test")
	}
}
