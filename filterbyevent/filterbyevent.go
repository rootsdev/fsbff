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
	"strings"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func isInPlace(place string, places []string) bool {
	for _, suffix := range places {
		if strings.HasSuffix(place, suffix) {
			return true
		}
	}
	return false
}

func processFile(filename string, eventType string, places []string, startYear int32, endYear int32) []string {
	var file io.ReadCloser
	var err error
	ids := make([]string, 0, 1000)

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

	for _, person := range fsPersons.Persons {
		match := false
		for _, fact := range person.Facts {
			if (eventType == "" || (fact.Type != nil && strings.HasSuffix(*fact.Type, eventType))) &&
			   (len(places) == 0 || (fact.Place != nil && isInPlace(*fact.Place, places))) &&
			   ((startYear == 0 && endYear == 9999) || (fact.Year != nil && *fact.Year >= startYear && *fact.Year <= endYear)) {
				match = true
				break
			}
		}
		if match {
			ids = append(ids, *person.Id)
		}
	}

	return ids
}

func processFiles(fileNames chan string, eventType string, places []string, startYear int32, endYear int32, results chan []string) {
	for fileName := range fileNames {
		results <- processFile(fileName, eventType, places, startYear, endYear)
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
var eventType = flag.String("t", "", "event type")
var place = flag.String("p", "", "place")
var startYear = flag.Int("s", 0, "start year")
var endYear = flag.Int("e", 9999, "start year")
var numWorkers = flag.Int("w", 1, "number of workers)")

func main() {
	flag.Parse()

	numCPU := runtime.NumCPU()
	fmt.Printf("Number of CPUs=%d\n", numCPU)
	runtime.GOMAXPROCS(int(math.Min(float64(numCPU), float64(*numWorkers))))

	numFiles, fileNames := getFilenames(*inFilename)

	fmt.Print("Processing files")
	results := make(chan []string)

	var places []string
	if place != nil {
		places = strings.Split(*place, "|")
	}

	for i := 0; i < *numWorkers; i++ {
		go processFiles(fileNames, *eventType, places, int32(*startYear), int32(*endYear), results)
	}

	out, err := os.Create(*outFilename)
	check(err)
	defer out.Close()
	buf := bufio.NewWriter(out)

	for i := 0; i < numFiles; i++ {
		ids := <-results
		for _, id := range ids {
			buf.WriteString(id)
			buf.WriteString("\n")
		}
		if i%100 == 0 {
			fmt.Print(".")
		}
	}

	buf.Flush()
	out.Sync()
}
