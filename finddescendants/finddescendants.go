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
	"sync"
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
  5. Iterate steps 2-4 until maxIterations has been reached or no new descendents have been added
  6. Write the descendants to the output file
*/

// global descendants map with a read-write mutex
var (
	descendants map[string]bool
	desdendantsMutex sync.RWMutex
)

func addDescendants(persons []*fs_data.FamilySearchPerson) {
	for _, person := range persons {
		desdendantsMutex.RLock()
		found := descendants[person.GetId()]
		desdendantsMutex.RUnlock()
		if found {
			desdendantsMutex.Lock()
			for _, child := range person.GetChildren() {
				descendants[child] = true
			}
			desdendantsMutex.Unlock()
		}
	}
}

func readDescendants(file *os.File) map[string]bool {
	descendants := make(map[string]bool)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		descendants[line] = true
	}
	return descendants
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func processFile(filename string) {
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

	protoBytes, err := ioutil.ReadAll(file)
	check(err)

	fsPersons := &fs_data.FamilySearchPersons{}
	err = proto.Unmarshal(protoBytes, fsPersons)
	check(err)

	addDescendants(fsPersons.GetPersons())
}

func processFiles(fileNames chan string, results chan int) {
	for fileName := range fileNames {
		processFile(fileName)
		results <- 0 // dummy value to signify file processing is complete
	}
}

var descendantsFilename = flag.String("d", "", "descendants filename")
var personsFilename = flag.String("p", "", "FS Persons proto filename or directory")
var outFilename = flag.String("o", "", "output filename or directory")
var maxIterations = flag.Int("m", 20, "maximum number of iterations")
var numWorkers = flag.Int("w", 1, "number of workers")

func main() {
	flag.Parse()

	numCPU := runtime.NumCPU()
	fmt.Printf("Number of CPUs=%d\n", numCPU)
	runtime.GOMAXPROCS(int(math.Min(float64(numCPU), float64(*numWorkers))))

	fileNames := make([]string, 0, 100000)

	fileInfo, err := os.Stat(*personsFilename)
	check(err)
	if fileInfo.IsDir() {
		fileInfos, err := ioutil.ReadDir(*personsFilename)
		check(err)
		for _, fileInfo := range fileInfos {
			fileNames = append(fileNames, *personsFilename + "/" + fileInfo.Name())
		}
	} else {
		fileNames = append(fileNames, *personsFilename)
	}

	fmt.Println("Reading descendants")
	descendantsFile, err := os.Open(*descendantsFilename)
	check(err)
	defer descendantsFile.Close()
	descendants = readDescendants(descendantsFile)

	results := make(chan int)
	fileNamesCh := make(chan string, 100000)
	var i int
	for i = 0; i < *numWorkers; i++ {
		go processFiles(fileNamesCh, results)
	}

	for iter := 0; iter < *maxIterations; iter++ {
		descendantsCount := len(descendants)
		fmt.Printf("Processing iteration %d #descendants=%d", iter, descendantsCount)

		// fill up the input channel
		for i = 0; i < len(fileNames); i++ {
			fileNamesCh <- fileNames[i]
		}

		// drain the output channel
		for i = 0; i < len(fileNames); i++ {
			<-results
			if i%1000 == 0 {
				fmt.Print(".")
			}
		}
		fmt.Println()

		// check if we should end early
		if descendantsCount == len(descendants) {
			fmt.Println("No more descendants found")
			break
		}
	}

	out, err := os.Create(*outFilename)
	check(err)
	defer out.Close()
	buf := bufio.NewWriter(out)

	for d := range descendants {
		buf.WriteString(fmt.Sprintf("%s\n", d))
	}
	buf.Flush()
	out.Sync()
}
