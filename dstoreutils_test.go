package main

import "testing"

func TestBasic(t *testing.T) {
	//SImple testung test
	if true {
		t.Errorf("Bad test")
	}
}
