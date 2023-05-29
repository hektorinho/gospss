## GoSPSS

This package can read an IBM Statistics data file in to golang to handle it any which way you want.

# Example 1:
Read data line by line.
```golang
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"github.com/hektorinho/gospss"
)

const TEST_FILE = "data/data7.sav"

func main() {
  	f, err := os.OpenFile(TEST_FILE, os.O_RDONLY, 0777)
	if err != nil {
		log.Panicf("failed to open %s ::: err >>> %s\n", TEST_FILE, err)
	}
	defer f.Close()
  
	r, err := NewReader(f)
	if err != nil {
		log.Panicf("failed to read %s ::: err >>> %s\n", TEST_FILE, err)
	}

	for i := 0; ; i++ {
		data, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panicf("err >>> %s\n", err)
		}
		
		// Handle your data ...
		fmt.Println(i, data)
	}
}
 ```
# Example 2:
Read all data and header information.
```golang
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"github.com/hektorinho/gospss"
)

const TEST_FILE = "data/data7.savz"

func main() {
  	f, err := os.OpenFile(TEST_FILE, os.O_RDONLY, 0777)
	if err != nil {
		log.Panicf("failed to open %s ::: err >>> %s\n", TEST_FILE, err)
	}
	defer f.Close()
  
	r, err := NewReader(f)
	if err != nil {
		log.Panicf("failed to read %s ::: err >>> %s\n", TEST_FILE, err)
	}

	// Exposes all header information
	header := r.Header()
	fmt.Println(header.Fileheader.ncases)

	// Reads the whole data file in to a list of gospss.Row.
	rows, err := r.ReadAll()
	if err != nil {
		log.Panicf("failed to read all records %s ::: err >>> %s", TEST_FILE, err)
	}
	for i, row := range rows[0:4] {
		fmt.Printf("row %d, data >>> %v\n", i, row)
	}
}
 ```
