package main

import (
	"bufio"
	"code.google.com/p/goprotobuf/proto"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/rootsdev/fsbff/fs_data"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
)

const YEAR_GRANULARITY = 10
const TOTAL = "TOTAL"

type Location struct {
	place string
	year int32
}

type Locations []Location
func NewLocations() Locations {
	return make(Locations, 0)
}
// Methods required by sort.Interface.
func (l Locations) Len() int {
    return len(l)
}
func (l Locations) Less(i, j int) bool {
    if l[i].place == l[j].place {
    	return l[i].year < l[j].year
    }
	return l[i].place < l[j].place
}
func (l Locations) Swap(i, j int) {
    l[i], l[j] = l[j], l[i]
}

type MigrationMap map[Location]map[string]int

func (mmap MigrationMap) add(key Location, value string, count int) {
    locations := mmap[key]
   	if locations == nil {
   		locations = make(map[string]int)
   		mmap[key] = locations
   	}
    locations[value] = locations[value] + count
}

// Maps "from" locations to "to" locations and vice-versa
type Migrations struct {
    singletons int
    immigrations MigrationMap  // map "to" Location to multiple "from" Locations with count each one occurred
    emigrations MigrationMap // map "from" Location to multiple "to" Locations with count each one occurred
}

func (m Migrations) add(from, to Location) {
    // standardize places into the correct levels
    stdFromLevels := stdPlace(from.place)
    stdToLevels := stdPlace(to.place)
    stdFromYear := from.year - from.year % YEAR_GRANULARITY
    stdToYear := to.year - to.year % YEAR_GRANULARITY

    // for each combination of levels: (from county, from state, from county) X (to county, to state, to country)
    for fromLevel := 0; fromLevel < len(stdFromLevels); fromLevel++ {
        // calculate the from location for this level
        stdFrom := Location{
            place: strings.Join(stdFromLevels[fromLevel:], ", "),
            year: stdFromYear,
        }

        for toLevel := 0; toLevel < len(stdToLevels); toLevel++ {
            // calculate the to location for this level
            stdTo := Location{
                place: strings.Join(stdToLevels[toLevel:], ", "),
                year: stdToYear,
            }

            // ignore P <=> P, Y,Z <=> Z, and Y,Z <=> X,Y,Z
            if stdFrom.place != stdTo.place &&
                !(len(stdFromLevels) - fromLevel > len(stdToLevels) - toLevel && strings.HasSuffix(stdFrom.place, stdTo.place)) &&
                !(len(stdFromLevels) - fromLevel < len(stdToLevels) - toLevel && strings.HasSuffix(stdTo.place, stdFrom.place)) {
                // add 1 to emigrations
                m.emigrations.add(stdFrom, stdTo.place, 1)

                // add 1 to immigrations
                m.immigrations.add(stdTo, stdFrom.place, 1)
            }

            // add 1 to immigrations total
            if fromLevel == 0 {
                m.immigrations.add(stdTo, TOTAL, 1)
            }
        }

        // add 1 to emigrations total
        m.emigrations.add(stdFrom, TOTAL, 1)
    }
}

func stdPlace(place string) []string {
    if place == "" {
        return []string{}
    }
    places := strings.Split(place, ",")
    var levels int
    if strings.TrimSpace(places[len(places) - 1]) == "United States" {
        levels = 3
    } else {
        levels = 2
    }
    if levels > len(places) {
        levels = len(places)
    }
    results := make([]string, 0, levels)
    for i := len(places) - levels; i < len(places); i++ {
        results = append(results, strings.TrimSpace(places[i]))
    }
    return results
}

func NewLocation(fact *fs_data.FSFact) Location {
	year := *fact.Year
	return Location{*fact.Place, year}
}

func processFile(filename string) Migrations {
	fsPersons := readPersons(filename)
	
	migrations := Migrations {
        immigrations: make(MigrationMap),
        emigrations: make(MigrationMap),
    }

	for _, person := range fsPersons.Persons {
		locations := NewLocations()
		for _, fact := range person.Facts {
			if fact.Place != nil && fact.Year != nil {
				locations = append(locations, NewLocation(fact))
			}
		}
		if len(locations) <= 1 {
            migrations.singletons++
			continue
		}
		sort.Sort(locations)
		// If place changes from one location to the next, we have
		// a migration. Record the most recent location as "from"
		// and new location as "to".
		//fmt.Println("Person", person)
		prev := Location{}
		for _, location := range locations {
			if prev.year >= 1500 && prev.year <= 2015 &&
                location.year >= 1500 && location.year <= 2015 {
                // move the migration test into the add function so we can calculate "total"'s
                //migrated(prev.place, location.place) {
				migrations.add(prev, location)
				//fmt.Printf("Migrated from: %v to %v (%d migrations)\n", prev, location, migrations[prev][location])
			}
			prev = location
		}
	}
	return migrations
}

// Determines if the 'from' and 'to' strings represent a migration, that
// is, if the locales are sufficiently different.
// Compare the last 3 locales if U.S., last 2 otherwise.
func migrated(from, to string) bool {
	if from == to {
		return false
	}
	fromPlaces := strings.Split(from, ",")
	toPlaces := strings.Split(to, ",")
	if len(fromPlaces) <= 1 || len(toPlaces) <= 1 {
		return false
	}
	// check country
	if !compare(fromPlaces[len(fromPlaces)-1], toPlaces[len(toPlaces)-1]) {
		return true
	}
	if len(fromPlaces) < 2 || len(toPlaces) < 2 {
		return false
	}
	// check state
	if !compare(fromPlaces[len(fromPlaces)-2], toPlaces[len(toPlaces)-2]) {
		return true
	}
	// If US also check county
	if compare(fromPlaces[len(fromPlaces) - 1], "United States") {
		if len(fromPlaces) < 3 || len(toPlaces) < 3 {
			return false
		}
		if !compare(fromPlaces[len(fromPlaces)-3], toPlaces[len(toPlaces)-3]) {
			return true
		}
	}
	return false
}

// Simple string compare that trims spaces and ignores case.
// As an oddity to make the above logic simpler, empty strings always compare true.
func compare(a, b string) bool {
	a1 := strings.ToLower(strings.TrimSpace(a))
	b1 := strings.ToLower(strings.TrimSpace(b))
	if a1 == "" || b1 == "" {
		return true
	}
	return a1 == b1
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func readPersons(filename string) *fs_data.FamilySearchPersons {
	var file io.ReadCloser
	var err error

	file, err = os.Open(filename)
	check(err)
	defer file.Close()

	if strings.HasSuffix(filename, ".gz") {
		file, err = gzip.NewReader(file)
		check(err)
		defer file.Close()
	}

	bytes, err := ioutil.ReadAll(file)
	check(err)

	fsPersons := &fs_data.FamilySearchPersons{}
	err = proto.Unmarshal(bytes, fsPersons)
	check(err)
	
	return fsPersons
}

func processFiles(fileNames chan string, results chan Migrations) {
	for fileName := range fileNames {
		//fmt.Printf("Processing file: %s", fileName)
		m := processFile(fileName)
		//fmt.Printf("; found %d immigration and %d emigration starts", len(m.immigrations), len(m.emigrations))
		results <- m
	}
}

func getFilenames(filename string) (int, chan string) {
	numFiles := 0
	fileNames := make(chan string, 100000)
	fileInfo, err := os.Stat(filename)
	check(err)
	if fileInfo.IsDir() {
		fileInfos, err := ioutil.ReadDir(filename)
		check(err)
		for _, fileInfo := range fileInfos {
			fileNames <- filename + "/" + fileInfo.Name()
			numFiles++
		}
	} else {
		fileNames <- filename
		numFiles++
	}
	close(fileNames)

	return numFiles, fileNames
}

func writeMigrations(migrations map[Location]map[string]int, label, filename string) {
    out, err := os.Create(filename)
   	check(err)
   	defer out.Close()
   	buf := bufio.NewWriter(out)
   	// Get all the locations and sort them, the write migrations
   	locs := NewLocations()
   	for l, _ := range migrations {
   		locs = append(locs, l)
   	}
   	sort.Sort(locs)
   	for _, loc := range locs {
   		buf.WriteString(fmt.Sprintf(label, loc))
   		for l, c := range migrations[loc] {
   			buf.WriteString(fmt.Sprintf(" %s (%d);", l, c))
   		}
   		buf.WriteString("\n")
   	}
   	buf.Flush()
   	out.Sync()
}

func countTotals(m MigrationMap) int {
    var total int
    for _, toMap := range m {
        total += toMap[TOTAL]
    }
    return total
}

var inFilename = flag.String("i", "", "input filename or directory")
var immigrationFilename = flag.String("im", "", "output filename for immigrations")
var emigrationFilename = flag.String("em", "", "output filename for emigrations")
var numWorkers = flag.Int("w", 1, "number of workers)")

func main() {
	flag.Parse()

	numCPU := runtime.NumCPU()
	fmt.Printf("Number of CPUs=%d\n", numCPU)
	runtime.GOMAXPROCS(int(math.Min(float64(numCPU), float64(*numWorkers))))

	numFiles, fileNames := getFilenames(*inFilename)

	fmt.Print("Processing files")
	results := make(chan Migrations)

	for i := 0; i < *numWorkers; i++ {
		go processFiles(fileNames, results)
	}

	// Merge all the resulting migration maps
    migrations := Migrations {
        singletons: 0,
        immigrations: make(MigrationMap),
        emigrations: make(MigrationMap),
    }
	for i := 0; i < numFiles; i++ {
		m := <-results
        migrations.singletons += m.singletons
        for from, toMap := range m.emigrations {
            for to, count := range toMap {
                migrations.emigrations.add(from, to, count)
            }
        }
        for to, fromMap := range m.immigrations {
            for from, count := range fromMap {
                migrations.immigrations.add(to, from, count)
            }
        }
        // monitor how many files have been processed
        if i % 100 == 0 {
            fmt.Print(".")
        }
	}
    totalImmigrations := countTotals(migrations.immigrations)
    totalEmigrations := countTotals(migrations.emigrations)
	fmt.Printf("\n\nTotal singletons: %d immigrations: %d emigrations %d\n", migrations.singletons, totalImmigrations, totalEmigrations)

    writeMigrations(migrations.immigrations, "To: %v From:", *immigrationFilename)
    writeMigrations(migrations.emigrations, "From: %v To:", *emigrationFilename)
}
