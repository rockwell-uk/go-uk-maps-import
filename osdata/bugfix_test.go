package osdata

import (
	"testing"
)

func TestFeatcodeFix(t *testing.T) {
	tests := map[string]struct {
		input     string
		outcome   int
		wantPanic bool
	}{
		"regular": {
			input:   "2500",
			outcome: 2500,
		},
		"buggy": {
			input:   "25200.0000",
			outcome: 25200,
		},
		"shouldn't happen": {
			input:     "abc",
			outcome:   25200,
			wantPanic: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); (r == nil) == tt.wantPanic {
					t.Errorf("featcodeFix() recover = %v, wantPanic = %v", r, tt.wantPanic)
				}
			}()

			actual := FeatcodeFix(tt.input)

			if tt.outcome != actual {
				t.Fatalf("FeatcodeFix: expected [%v], got [%v]", tt.outcome, actual)
			}
		})
	}
}

func TestInvalidUTF8Fix(t *testing.T) {
	tests := map[string]string{
		"Tobha Rònaigh": "Tobha Ronaigh",
	}

	for input, output := range tests {
		actual := InvalidUTF8Fix(input)

		if output != actual {
			t.Fatalf("TestInvalidUTF8Fix: expected [%v], got [%v]", output, actual)
		}
	}
}
