package main

import "fmt"

func main() {
	for i := range 1000 {
		fmt.Println(i)
	}

	for i := 1; i < 3; i++ {
		fmt.Println(i)
	}
}
