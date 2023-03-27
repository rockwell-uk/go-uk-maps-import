package sqlwriter

import (
	"os"
	"reflect"
	"testing"
)

func TestRemoveLastComma(t *testing.T) {
	tempFile := "./tmp.txt"
	defer os.Remove(tempFile)

	lines := []byte("hello\ngo,\n")
	err := os.WriteFile(tempFile, lines, 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = removeLastComma(tempFile)
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.ReadFile(tempFile)
	if err != nil {
		t.Fatal(err)
	}

	expected := []byte("hello\ngo\n")
	if !reflect.DeepEqual(expected, f) {
		t.Fatalf("expected %s\nactual %s", expected, f)
	}
}
