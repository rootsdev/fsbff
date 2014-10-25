package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"net/http"
	"encoding/json"
	"io/ioutil"
	"time"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type source struct {
	SourceDescription []*SourceDescription `json:"sourceDescriptions"`
}

type SourceDescription struct {
	About       string    `json:"about"`
	Citations   []*Value  `json:"citations"`
	Titles      []*Value  `json:"titles"`
}

type Value struct {
	Value string `json:"value"`
}

func fetch(client *http.Client, req *http.Request) ([]byte, error) {
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == 429 {
		fmt.Println("waiting")
		time.Sleep(1000 * time.Millisecond)
		return fetch(client, req)
	}
	if res.StatusCode > 299 {
		return nil, fmt.Errorf("Status code %d", res.StatusCode)
	}
	return ioutil.ReadAll(res.Body)
}

func fetchSource(client *http.Client, accessToken string, sourceId string) (*source, error) {
	req, err := http.NewRequest("GET", "https://familysearch.org/platform/sources/descriptions/"+sourceId, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/x-fs-v1+json")
	req.Header.Add("Authorization", "Bearer "+accessToken)
	body, err := fetch(client, req)
	if err != nil {
		return nil, err
	}
	source := &source{}
	if err = json.Unmarshal(body, source); err != nil {
		return nil, err
	}
	return source, nil
}

func fetchSources(accessToken string, sourceIds chan string, results chan string) {
	client := &http.Client{}
	for sourceId := range sourceIds {
		source, err := fetchSource(client, accessToken, sourceId)
		if err != nil {
			fmt.Printf("Error %v\n", err)
			continue
		}
		if len(source.SourceDescription) == 0 {
			fmt.Println("No sourceDescription")
			continue
		}
		sourceDescription := source.SourceDescription[0]
		title := ""
		if len(sourceDescription.Titles) > 0 {
			title = sourceDescription.Titles[0].Value
		}
		results <- sourceId + "|" + sourceDescription.About + "|" + title
	}
	results <- "__FINISHED__"
}

func readSourceIds(filename string, sourceIds chan string) {
	file, err := os.Open(filename)
	check(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		sourceIds <- scanner.Text()
	}
	check(scanner.Err())

	close(sourceIds)
}

var inFilename = flag.String("i", "", "input sourceIds filename")
var outFilename = flag.String("o", "", "output source filename")
var accessToken = flag.String("a", "", "access token")
var numWorkers = flag.Int("w", 1, "number of workers)")

func main() {
	flag.Parse()

	numCPU := runtime.NumCPU()
	fmt.Printf("Number of CPUs=%d\n", numCPU)
	runtime.GOMAXPROCS(int(math.Min(float64(numCPU), float64(*numWorkers))))

	file, err := os.Open(*inFilename)
	check(err)
	defer file.Close()

	sourceIds := make(chan string)

	go readSourceIds(*inFilename, sourceIds)

	fmt.Print("Processing files")
	results := make(chan string)

	for i := 0; i < *numWorkers; i++ {
		go fetchSources(*accessToken, sourceIds, results)
	}

	out, err := os.Create(*outFilename)
	check(err)
	defer out.Close()
	buf := bufio.NewWriter(out)

	finishedWorkers := 0
	cnt := 0
	for finishedWorkers < *numWorkers {
		collection := <-results
		if collection == "__FINISHED__" {
			finishedWorkers++
		} else {
			buf.WriteString(collection)
			buf.WriteString("\n")
			cnt++
			if cnt%1000 == 0 {
				fmt.Print(".")
			}
		}
	}

	buf.Flush()
	out.Sync()
}
