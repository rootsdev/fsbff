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

func processFile(filename string) map[string]int {
	var file io.ReadCloser
	var err error
	eventTypes := make(map[string]int)

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
		for _, fact := range person.Facts {
			factType := "nil"
			if fact.Type != nil {
				factType = *fact.Type
			}
			eventTypes[factType] = eventTypes[factType] + 1
		}
	}

	return eventTypes
}

func processFiles(fileNames chan string, results chan map[string]int) {
	for fileName := range fileNames {
		results <- processFile(fileName)
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

	fmt.Print("Processing files")
	results := make(chan map[string]int)

	for i := 0; i < *numWorkers; i++ {
		go processFiles(fileNames, results)
	}

	totalCounts := make(map[string]int)

	for i := 0; i < numFiles; i++ {
		eventTypes := <-results
		for k, v := range eventTypes {
			totalCounts[k] = totalCounts[k] + v
		}
		if i%100 == 0 {
			fmt.Print(".")
		}
	}

	out, err := os.Create(*outFilename)
	check(err)
	defer out.Close()
	buf := bufio.NewWriter(out)

	for k, v := range totalCounts {
		buf.WriteString(fmt.Sprintf("%09d,%s\n", v, k))
	}
	buf.Flush()
	out.Sync()
}
