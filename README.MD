## GoSPSS

This package can read an IBM Statistics data file in to golang to handle it any which way you want.

# Example code:
```golang
package main

import "github.com/hektorinho/gospss"

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
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panicf("err >>> %s\n", err)
		}
		// fmt.Println(i, data)
	}
}
 ```