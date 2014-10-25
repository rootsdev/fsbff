package main

import (
	"bufio"
	"code.google.com/p/goprotobuf/proto"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/rootsdev/fsbff/fs_data"
	"io/ioutil"
	"log"
	"math"
	"os"
	"runtime"
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

func addDescendants(descendants map[string]bool, persons map[string]*fs_data.FamilySearchPerson) {
	for _, person := range persons {
		if descendants[person.GetId()] {
			for _, child := range person.GetChildren() {
				descendants[child] = true
			}
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

func processFile(filename string, descendants map[string]bool) (recordCount int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Error opening %s %v", filename, err)
		return 0
	}
	defer file.Close()

	if filename[len(filename)-3:] == ".gz" {
		file, err := gzip.NewReader(file)
		if err != nil {
			log.Printf("Error unzipping %s %v", filename, err)
			return 0
		}
		defer file.Close()
	}

	protoBytes, err := ioutil.ReadAll(file)
	check(err)

	fsPersons := &fs_data.FamilySearchPersons{}
	err = proto.Unmarshal(protoBytes, fsPersons)
	check(err)
	
	persons := make(map[string]*fs_data.FamilySearchPerson)
	for _, person := range fsPersons.GetPersons() {
		persons[person.GetId()] = person
	}
	
	addDescendants(descendants, persons)
	return len(persons)
}

func processFiles(fileNames chan string, descendants map[string]bool, results chan int) {
	for fileName := range fileNames {
		results <- processFile(fileName, descendants)
	}
}

var descendantsFilename = flag.String("d", "", "descendants filename")
var personsFilename = flag.String("p", "", "FS Persons proto filename or directory")
var outFilename = flag.String("o", "", "output filename or directory")
var numWorkers = flag.Int("w", 1, "number of workers")

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
			fileNames <- *personsFilename + "/" + fileInfo.Name()
			numFiles++
		}
	} else {
		fileNames <- *personsFilename
		numFiles++
	}
	close(fileNames)

	fmt.Println("Reading descendants")
	descendantsFile, err := os.Open(*descendantsFilename)
	check(err)
	defer descendantsFile.Close()
	descendants := readDescendants(descendantsFile)

	fmt.Print("Processing files")
	results := make(chan int)

	for i := 0; i < *numWorkers; i++ {
		go processFiles(fileNames, descendants, results)
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

	out, err := os.Create(*outFilename)
	check(err)
	defer out.Close()
	buf := bufio.NewWriter(out)

	for d := range descendants {
		buf.WriteString(fmt.Sprintf("%s\n", d))
	}
	buf.Flush()
	out.Sync()

	fmt.Printf("\nTotal files=%d records=%d\n", filesProcessed, recordsProcessed)
}
