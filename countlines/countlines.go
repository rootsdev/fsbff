package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var inFilename = flag.String("i", "", "input filename")
var outFilename = flag.String("o", "", "output filename")

func main() {
	flag.Parse()

	file, err := os.Open(*inFilename)
	check(err)
	defer file.Close()

	lines := make(map[string]int)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		// temporary fixup for bad lines
		pos := strings.Index(line, ",\"")
		if pos > 0 {
			line = line[0:pos]
		}

		lines[line]++
	}

	out, err := os.Create(*outFilename)
	check(err)
	defer out.Close()
	buf := bufio.NewWriter(out)

	for k, v := range lines {
		buf.WriteString(fmt.Sprintf("%06d %s\n", v, k))
	}

	buf.Flush()
	out.Sync()
}
