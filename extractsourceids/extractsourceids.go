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

func processFile(filename string, personIds map[string]bool) []string {
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
		if personIds == nil || personIds[*person.Id] {
			for _, source := range person.Sources {
				ids = append(ids, *source.SourceId)
			}
		}  
	}

	return ids
}

func processFiles(fileNames chan string, personIds map[string]bool, results chan []string) {
	for fileName := range fileNames {
		results <- processFile(fileName, personIds)
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
var personIdsFilename = flag.String("p", "", "personIds filename")
var numWorkers = flag.Int("w", 1, "number of workers)")

func readPersonIds(filename string) map[string]bool {
	result := make(map[string]bool)

	file, err := os.Open(filename)
	check(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		result[scanner.Text()] = true
	}
	check(scanner.Err())

	return result
}

func main() {
	flag.Parse()

	numCPU := runtime.NumCPU()
	fmt.Printf("Number of CPUs=%d\n", numCPU)
	runtime.GOMAXPROCS(int(math.Min(float64(numCPU), float64(*numWorkers))))

	var personIds map[string]bool
	if personIdsFilename != nil {
		personIds = readPersonIds(*personIdsFilename)
	}

	numFiles, fileNames := getFilenames(*inFilename)

	fmt.Print("Processing files")
	results := make(chan []string)

	var i int
	for i = 0; i < *numWorkers; i++ {
		go processFiles(fileNames, personIds, results)
	}

	out, err := os.Create(*outFilename)
	check(err)
	defer out.Close()
	buf := bufio.NewWriter(out)

	for i = 0; i < numFiles; i++ {
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

