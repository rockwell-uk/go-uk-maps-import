package osdata

import (
	"strconv"
	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

func FeatcodeFix(value string) int {
	var err error

	f, err := strconv.ParseFloat(value, 64)
	if err == nil {
		return int(f)
	}

	panic(value)
}

func InvalidUTF8Fix(value string) string {
	var normalizer = transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	value, _, _ = transform.String(normalizer, value)

	return value
}
