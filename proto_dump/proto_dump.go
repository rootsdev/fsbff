package main

import (
	"code.google.com/p/goprotobuf/proto"
	"flag"
	"fmt"
	"github.com/rootsdev/fsbff/fs_data"
	"io/ioutil"
	"log"
	"os"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	var numRecords = flag.Int("n", 10, "number of records to dump")
	var field = flag.String("f", "", "field to dump: [a]ll, [i]d")
	flag.Parse()
	
	file, err := os.Open(flag.Arg(0))
	check(err)
	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	check(err)

	fsPersons := &fs_data.FamilySearchPersons{}
	err = proto.Unmarshal(bytes, fsPersons)
	check(err)

	for i := 0; i < *numRecords; i++ {
		switch *field {
		case "i":
			fmt.Printf("%s\n", fsPersons.Persons[i].GetId())
		default:
			fmt.Printf("fsPersons[%d]=%+v\n\n", i, fsPersons.Persons[i])
		}
	}
}
