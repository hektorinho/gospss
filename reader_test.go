package gospss

import (
	"fmt"
	"io"
	"log"
	"os"
	"testing"
)

const (
	TEST_FILE      = "data/data7.sav"
	TEST_FILE_GZIP = "data/data7.zsav"
)

func TestReader(t *testing.T) {
	f, err := os.OpenFile(TEST_FILE_GZIP, os.O_RDONLY, 0777)
	if err != nil {
		t.Errorf("failed to open %s ::: err >>> %s", TEST_FILE, err)
	}
	defer f.Close()

	r, err := NewReader(f)
	if err != nil {
		t.Errorf("failed to read %s ::: err >>> %s", TEST_FILE, err)
	}

	for i := 0; ; i++ {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panicln("err >>>", err)
		}
		// fmt.Println(i, data)
	}
}
func TestReaderAll(t *testing.T) {
	f, err := os.OpenFile(TEST_FILE_GZIP, os.O_RDONLY, 0777)
	if err != nil {
		t.Errorf("failed to open %s ::: err >>> %s", TEST_FILE, err)
	}
	defer f.Close()

	r, err := NewReader(f)
	if err != nil {
		t.Errorf("failed to read %s ::: err >>> %s", TEST_FILE, err)
	}

	rows, err := r.ReadAll()
	if err != nil {
		t.Errorf("failed to read all records %s ::: err >>> %s", TEST_FILE, err)
	}
	for i, row := range rows[0:4] {
		fmt.Printf("row %d, data >>> %v\n", i, row)
	}
}

// TODO: more serious unit testing for each function...
// var readTests = []struct {
// 	Name   string
// 	Input  []byte
// 	Output string
// }{
// 	{
// 		Name: "Fileheader",
// 		Input: []byte{36, 70, 76, 50, 64, 40, 35, 41, 32, 73, 66, 77, 32, 83, 80, 83, 83, 32, 83, 84, 65, 84, 73, 83, 84, 73, 67, 83, 32, 54, 52, 45, 98, 105, 116, 32, 77, 83, 32, 87, 105,
// 			110, 100, 111, 119, 115, 32, 50, 50, 46, 48, 46, 48, 46, 48, 32, 32, 32, 32, 32, 32, 32, 32, 32, 2, 0, 0, 0, 160, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 158, 14, 0, 0, 0, 0, 0, 0, 0, 0, 89, 64, 50, 56, 32, 78, 111, 118, 32, 49, 54, 49, 56, 58, 49, 56, 58, 52, 49, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
// 			32, 32, 32, 32, 0, 0, 0},
// 		Output: "28 Nov 16",
// 	},
// }
