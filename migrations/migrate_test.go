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

func TestMigrationMapAdd(t *testing.T) {
    var migrations = []struct {
        key Location
        value string
        count int
    }{
        {Location{"p1", 1}, "p2", 10},
        {Location{"p1", 1}, "p2", 20},
        {Location{"p1", 1}, "p3", 40},
        {Location{"p2", 1}, "p1", 50},
    }
    mmap := make(MigrationMap)
    for _, m := range migrations {
        mmap.add(m.key, m.value, m.count)
    }
    var tests = []struct {
        key Location
        value string
        count int
    }{
        {Location{"p1", 1}, "p2", 30},
        {Location{"p1", 1}, "p3", 40},
    }
    for _, test := range tests {
        sum := mmap[test.key][test.value]
        if sum != test.count {
            t.Errorf("TestMigrationMapAdd got %d; want %d", sum, test.count)
        }
    }
}

func TestStdPlace(t *testing.T) {
    var tests = []struct {
        place string
        stdLevels []string
    }{
        {"Provo, Utah, Utah, United States", []string{"Utah", "Utah", "United States"}},
        {"London, Greater London, England", []string{"Greater London", "England"}},
        {"Minnesota, United States", []string{"Minnesota", "United States"}},
        {"Sussex, England", []string{"Sussex", "England"}},
    }
    for _, test := range tests {
        stdLevels := stdPlace(test.place)
        if !equalStringSlice(stdLevels, test.stdLevels) {
            t.Errorf("TestStdPlace got %v; want %v", stdLevels, test.stdLevels)
        }
    }
}

func equalStringSlice(s1 []string, s2 []string) bool {
    if len(s1) != len(s2) {
        return false
    }
    for i := range s1 {
        if s1[i] != s2[i] {
            return false
        }
    }
    return true
}

func TestMigrationAdd(t *testing.T) {
    var migrations = []struct {
        from Location
        to Location
    }{
        {Location{"Provo, Utah, Utah, United States", 1840}, Location{"Salt Lake, Utah, United States", 1850}},
        {Location{"Orem, Utah, Utah, United States", 1841}, Location{"Salt Lake, Utah, United States", 1850}},
        {Location{"Nephi, Juab, Utah, United States", 1840}, Location{"Ramsey, Minnesota, United States", 1850}},
        {Location{"Salt Lake, Utah, United States", 1842}, Location{"Minnesota, United States", 1850}},
    }
    var emigrationTests = []struct {
        from Location
        to string
        count int
    }{
        {Location{"Utah, Utah, United States", 1840}, "Salt Lake, Utah, United States", 2},
        {Location{"Juab, Utah, United States", 1840}, "Ramsey, Minnesota, United States", 1},
        {Location{"Juab, Utah, United States", 1840}, "Minnesota, United States", 1},
        {Location{"Salt Lake, Utah, United States", 1840}, "Minnesota, United States", 1},
        {Location{"Utah, United States", 1840}, "Ramsey, Minnesota, United States", 1},
        {Location{"Utah, United States", 1840}, "Minnesota, United States", 2},
        {Location{"Utah, Utah, United States", 1840}, "TOTAL", 2},
        {Location{"Juab, Utah, United States", 1840}, "TOTAL", 1},
        {Location{"Salt Lake, Utah, United States", 1840}, "TOTAL", 1},
        {Location{"Utah, United States", 1840}, "TOTAL", 4},
        {Location{"United States", 1840}, "TOTAL", 4},
    }
    totalEmigrations := 20

    var immigrationTests = []struct {
        to Location
        from string
        count int
    }{
        {Location{"Salt Lake, Utah, United States", 1850}, "Utah, Utah, United States", 2},
        {Location{"Ramsey, Minnesota, United States", 1850}, "Juab, Utah, United States", 1},
        {Location{"Minnesota, United States", 1850}, "Juab, Utah, United States", 1},
        {Location{"Minnesota, United States", 1850}, "Salt Lake, Utah, United States", 1},
        {Location{"Ramsey, Minnesota, United States", 1850}, "Utah, United States", 1},
        {Location{"Minnesota, United States", 1850}, "Utah, United States", 2},
        {Location{"Salt Lake, Utah, United States", 1850}, "TOTAL", 2},
        {Location{"Utah, United States", 1850}, "TOTAL", 2},
        {Location{"Ramsey, Minnesota, United States", 1850}, "TOTAL", 1},
        {Location{"Minnesota, United States", 1850}, "TOTAL", 2},
        {Location{"United States", 1850}, "TOTAL", 4},
    }
    totalImmigrations := 19

    m := Migrations {
        immigrations: make(MigrationMap),
        emigrations: make(MigrationMap),
    }
    for _, migration := range migrations {
        m.add(migration.from, migration.to)
    }

    for _, test := range emigrationTests {
        sum := m.emigrations[test.from][test.to]
        if sum != test.count {
            t.Errorf("TestMigrationAdd emigrations %s %d -> %s got %d; want %d",
               test.from.place, test.from.year, test.to, sum, test.count)
        }
    }
    for _, test := range immigrationTests {
        sum := m.immigrations[test.to][test.from]
        if sum != test.count {
            t.Errorf("TestMigrationAdd immigrations %s %d <- %s got %d; want %d",
                test.to.place, test.to.year, test.from, sum, test.count)
        }
    }

    var cnt int
    for _, v := range m.emigrations {
        for _, c := range v {
            cnt += c
        }
    }
    if cnt != totalEmigrations {
        t.Errorf("TestMigrationAdd total emigrations got %d; want %d", cnt, totalEmigrations)
    }

    cnt = 0
    for _, v := range m.immigrations {
        for _, c := range v {
            cnt += c
        }
    }
    if cnt != totalImmigrations {
        t.Errorf("TestMigrationAdd total immigrations got %d; want %d", cnt, totalImmigrations)
    }
}