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

const YEAR_GRANULARITY = 20

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
    if l[i].year == l[j].year {
    	return l[i].place < l[j].place
    }
	return l[i].year < l[j].year
}
func (l Locations) Swap(i, j int) {
    l[i], l[j] = l[j], l[i]
}

// Maps "from" Location to multiple "to" Locations with count each one occurred
type Migrations map[Location]map[Location]int
func (m Migrations) add(from, to Location, count int) {
    stdFrom := Location{
        place: stdPlace(from.place),
        year: from.year - from.year % YEAR_GRANULARITY,
    }
    stdTo := Location{
        place: stdPlace(to.place),
        year: to.year - to.year % YEAR_GRANULARITY,
    }
	f := m[stdFrom]
	if f == nil {
		f = make(map[Location]int)
		m[stdFrom] = f
	}
	f[stdTo] = f[stdTo] + count
}

func stdPlace(place string) string {
    places := strings.Split(place, ",")
    if len(places) <= 1 {
        return place
    }
    var levels int
    if strings.TrimSpace(places[len(places) - 1]) == "United States" {
        levels = 3
    } else {
        levels = 2
    }
    if levels > len(places) {
        levels = len(places)
    }
    var results []string
    for i := len(places) - levels; i < len(places); i++ {
        results = append(results, strings.TrimSpace(places[i]))
    }
    return strings.Join(results, ", ")
}

func NewLocation(fact *fs_data.FSFact) Location {
	year := *fact.Year
	return Location{*fact.Place, year}
}

func processFile(filename string) Migrations {
	fsPersons := readPersons(filename)
	
	migrations := make(Migrations)
	for _, person := range fsPersons.Persons {
		locations := NewLocations()
		for _, fact := range person.Facts {
			if fact.Place != nil && fact.Year != nil {
				locations = append(locations, NewLocation(fact))
			}
		}
		if len(locations) <= 1 {
			continue
		}
		sort.Sort(locations)
		// If place changes from one location to the next, we have
		// a migration. Record the most recent location as "from"
		// and new location as "to".
		//fmt.Println("Person", person)
		prev := Location{}
		for _, location := range locations {
			if migrated(prev.place, location.place) {
				migrations.add(prev, location, 1)
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
		fmt.Printf("Processing file: %s", fileName)
		m := processFile(fileName)
		fmt.Printf("; found %d migration starts", len(m))
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

var inFilename = flag.String("i", "", "input filename or directory")
var outFilename = flag.String("o", "", "output filename")
var numWorkers = flag.Int("w", 1, "number of workers)")

func main() {
	flag.Parse()

	numCPU := runtime.NumCPU()
	fmt.Printf("Number of CPUs=%d\n", numCPU)
	runtime.GOMAXPROCS(int(math.Min(float64(numCPU), float64(*numWorkers))))

	numFiles, fileNames := getFilenames(*inFilename)

	fmt.Println("Processing files")
	results := make(chan Migrations)

	for i := 0; i < *numWorkers; i++ {
		go processFiles(fileNames, results)
	}

	// Merge all the resulting migration maps
	migrations := make(Migrations)
	for i := 0; i < numFiles; i++ {
		m := <-results
		for from, toMap := range m {
			for to, count := range toMap {
				migrations.add(from, to, count)
			}
		}
	}
	fmt.Println("\n\nTotal:", len(migrations))
	
	out, err := os.Create(*outFilename)
	check(err)
	defer out.Close()
	buf := bufio.NewWriter(out)
	// Get all the "from" locations and sort them, the write migrations
	locs := NewLocations()
	for l, _ := range migrations {
		locs = append(locs, l)
	}
	sort.Sort(locs)
	for _, loc := range locs {
		buf.WriteString(fmt.Sprintf("From: %v To:", loc))
		for l, c := range migrations[loc] {
			buf.WriteString(fmt.Sprintf(" %v (%d)", l, c))
		}
		buf.WriteString("\n")
	}
	buf.Flush()
	out.Sync()
}
