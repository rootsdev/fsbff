package main

import (
	"testing"
)

func TestMigrated(t *testing.T) {
	var tests = []struct {
		inFrom string
		inTo   string
		out    bool
	}{
		{"", "", false},
		{"", "a", false},
		{"a", "", false},
		{"abc", "a,b,c", false},
		{"def", "def", false},
		{"a, b", "a, b", false}, // non-US
		{"a ,  b ", "a, b", false}, // check space trimming
		{"A, B", "a, b", false}, // check capitalization
		{",, a, b", ", b", false}, // check missing fields
		{"a, b", "c, a, b", false}, // non-US, one extra level
		{"a, b, c, d", "e, a, b, c, d", false}, // non-US, extra levels
		{"a, b, c", "a, b, United States", true}, // one US, one not
		{"a, b, United States", "a, b, c", true}, // one US, one not
		{"a, b, United States", "a, b, United States", false}, // US
		{"a, b, United States", "c, a, b, United States", false}, // US, one extra level
		{"a, b, c, United States", "d, a, b, c, United States", false}, // US, extra levels
		{"a, b, c, United States", "d, b, c, United States", false}, // US, city differs
		{"a, b", "c, b", true}, // non-US, state differs
		{"a, b", "a, c", true}, // non-US, country differs
		{"a, b", "d, c, b", true}, // non-US, one extra level, state differs
		{"a, b", "d, a, c", true}, // non-US, one extra level, country differs
		{"a, b, c, d", "e, a, b, f, d", true}, // non-US, extra levels, state differs
		{"a, b, c, d", "e, a, b, c, f", true}, // non-US, extra levels, country differs
		{"a, b, United States", "c, b, United States", true}, // US, county differs
		{"a, b, United States", "a, c, United States", true}, // US, state differs
		{"a, b, United States", "c, d, b, United States", true}, // US, one extra level, county differs
		{"a, b, United States", "c, a, d, United States", true}, // US, one extra level, state differs
		{"a, b, c, United States", "d, a, e, c, United States", true}, // US, extra levels, county differs
		{"a, b, c, United States", "d, a, b, e, United States", true}, // US, extra levels, state differs
	}
	for _, test := range tests {
		actual := migrated(test.inFrom, test.inTo)
		if actual != test.out {
			t.Errorf("migrated(%q, %q) = %v; want %v", test.inFrom, test.inTo, actual, test.out)
		}
	}
}
