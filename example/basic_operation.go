package main

import (
	bitcask "bitcask"
	"fmt"
)

func main() {
	opts := bitcask.DefaultDBOptions
	opts.DirPath = "/tmp/bitcask-go"

	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	// err = db.Put([]byte("name"), []byte("enophan"))
	// if err != nil {
	// 	panic(err)
	// }

	value, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("value:", string(value))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
