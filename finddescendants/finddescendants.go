package main

import (
	"bufio"
	"bytes"
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

/*
Finds all the descendants of the people in a file.
The descendants must be listed in a flat text file with one person ID per line.
FamilySearch people are in proto bufs.

The most straight-forward way to find the descendants is to read all the FS people into
a map of person ID and person. Then scan the descendants file one ID at a time. For each
descendant, look up the person in the FS people map, gather all that person's children, and
add them to the end of the descendants list. Continue to the end.

However, there are two complications that make this impractical. First, the FS people file
has cycles, so the above algorithm may never complete. Second, the number of FS people is so
large that the person map will not all fit into memory.

This package instead implements the algorithm as follows:
  1. Read all the descendants into a set
  2. Read a single proto file of FS people and create a map of person ID and person
  3. If the person is in the descendants set, add all its children to the set
  4. Repeat steps 2 and 3 until all the proto files have been processed
  5. Write the descendants to the output file
The program should now be re-run with the output file from step 5 as the new input file
to step 1. Repeat until no new descendants are found.

To do: Automate re-running the program. Step 5 should be simply, reset the FS people files and
re-run steps 2-5 with the current set of descendants. Repeat until no new descendants are found.
*/

var descendants map[string][]string

func addDescendants(persons []*fs_data.FamilySearchPerson) {
	for i, person := range persons {
		fmt.Printf("fsPersons[%d]=%+v\n\n", i, person)
	}
}

func readSourceRefs(file *os.File) map[string][]string {
	sourceRefs := make(map[string][]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		sourceRefs[line] = make([]string, 0)
	}
	return sourceRefs
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func processFile(filename string, gzipOutput bool) (recordCount int) {
	inOut := strings.SplitN(filename, "\t", 2)
	inFilename := inOut[0]
	outFilename := inOut[1]
	var file io.ReadCloser
	var err error

	file, err = os.Open(inFilename)
	if err != nil {
		log.Printf("Error opening %s %v", inFilename, err)
		return 0
	}
	defer file.Close()

	if inFilename[len(inFilename)-3:] == ".gz" {
		file, err = gzip.NewReader(file)
		if err != nil {
			log.Printf("Error unzipping %s %v", inFilename, err)
			return 0
		}
		defer file.Close()
	}

	protoBytes, err := ioutil.ReadAll(file)
	check(err)

	fsPersons := &fs_data.FamilySearchPersons{}
	err = proto.Unmarshal(protoBytes, fsPersons)
	check(err)
	
	addDescendants(fsPersons.GetPersons())

	b := make([]byte, 0)
	if gzipOutput {
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		w.Write(b)
		w.Close()
		b = buf.Bytes()
	}

	err = ioutil.WriteFile(outFilename, b, 0644)
	if err != nil {
		log.Printf("Error writing %s %v", outFilename, err)
		return 0
	}

	return
}

func processFiles(fileNames chan string, results chan int, gzipOutput bool) {
	for fileName := range fileNames {
		results <- processFile(fileName, gzipOutput)
	}
}

var descendantsFilename = flag.String("d", "", "descendants filename or directory")
var personsFilename = flag.String("p", "", "FS Persons proto filename or directory")
var outFilename = flag.String("o", "", "output filename or directory")
var numWorkers = flag.Int("w", 1, "number of workers")
var gzipOutput = flag.Bool("z", false, "gzip output")

func main() {
	flag.Parse()

	numCPU := runtime.NumCPU()
	fmt.Printf("Number of CPUs=%d\n", numCPU)
	runtime.GOMAXPROCS(int(math.Min(float64(numCPU), float64(*numWorkers))))

	numFiles := 0
	fileNames := make(chan string, 100000)
	fileInfo, err := os.Stat(*personsFilename)
	check(err)
	if fileInfo.IsDir() {
		fileInfos, err := ioutil.ReadDir(*personsFilename)
		check(err)
		for _, fileInfo := range fileInfos {
			start := 0
			if strings.HasPrefix(fileInfo.Name(), "gedcomxb.") {
				start = len("gedcomxb.")
			}
			end := strings.Index(fileInfo.Name(), ".xml")
			suffix := ""
			if *gzipOutput {
				suffix = ".gz"
			}
			fileNames <- *personsFilename + "/" + fileInfo.Name() + "\t" +
				*outFilename + "/" + fileInfo.Name()[start:end] + ".protobuf" + suffix
			numFiles++
		}
	} else {
		fileNames <- *personsFilename + "\t" + *outFilename
		numFiles++
	}
	close(fileNames)

	fmt.Println("Reading descendants")
	descendantsFile, err := os.Open(*descendantsFilename)
	check(err)
	defer descendantsFile.Close()
	descendants = readSourceRefs(descendantsFile)

	fmt.Print("Processing files")
	results := make(chan int)

	for i := 0; i < *numWorkers; i++ {
		go processFiles(fileNames, results, *gzipOutput)
	}

	recordsProcessed := 0
	filesProcessed := 0
	for i := 0; i < numFiles; i++ {
		recordsProcessed += <-results
		filesProcessed++
		if filesProcessed%100 == 0 {
			fmt.Print(".")
		}
	}
	fmt.Printf("\nTotal files=%d records=%d\n", filesProcessed, recordsProcessed)
}
