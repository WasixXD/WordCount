package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"slices"
	"sort"
	"sync"
)

var fileNames = []string{
	"first.txt",
	"second.txt",
	"third.txt",
	"fourth.txt",
	"fifth.txt",
}

type Master struct {
	filenames     []string
	mapper_files  []string
	mapper_groupt sync.WaitGroup
	lock          sync.Mutex
}

func (m *Master) AddMapper(filepath string) {
	m.lock.Lock()
	m.mapper_files = append(m.mapper_files, filepath)
	m.lock.Unlock()
}

func randomLetter() string {
	return string(rune(65+rand.Intn(26))) + "\n"
}

func createFiles() {
	wait := sync.WaitGroup{}
	for _, file := range fileNames {
		_, err := os.Create(file)
		if err != nil {
			log.Println(err)
		}

		wait.Add(1)
		go func(s string) {
			defer wait.Done()
			for i := 0; i < 100_000; i++ {
				file, _ := os.OpenFile(s, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

				_, e := file.WriteString(randomLetter())

				if e != nil {
					log.Println("Err: ", e)
				}

				file.Close()
			}
		}(file)
	}
	wait.Wait()
}

func Map(key string, m *Master) {
	// key: filename
	file, err := os.ReadFile(key)

	if err != nil {
		log.Println("Err on reading: ", err)
	}

	json_file, _ := os.Create("out-" + key + ".json")
	m.AddMapper(json_file.Name())
	json_ref := json.NewEncoder(json_file)
	defer json_file.Close()

	for _, letter := range file {
		s := string(letter)
		if s != "\n" {
			json_ref.Encode(map[string]interface{}{"letter": s, "value": 1})
		}
	}
	m.mapper_groupt.Done()
}

func Reduce(values []map[string]int, m *Master) {
	defer m.mapper_groupt.Done()

	table := make(map[string]int)
	keys := make([]string, 0, 26)

	for _, result := range values {
		for k, v := range result {
			table[k] += v
			if !slices.Contains(keys, k) {
				keys = append(keys, k)
			}
		}
	}

	sort.Strings(keys)

	file, err := os.OpenFile("out-final.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("error: ", err)
	}

	defer file.Close()

	for _, key := range keys {
		file.WriteString(fmt.Sprintf("%v %v\n", key, table[key]))
	}

}

func main() {
	createFiles()

	m := Master{filenames: fileNames, mapper_files: []string{}, mapper_groupt: sync.WaitGroup{}, lock: sync.Mutex{}}

	// Map part
	for _, filename := range m.filenames {
		m.mapper_groupt.Add(1)
		go Map(filename, &m)
	}

	m.mapper_groupt.Wait()

	// shuffle

	reducers := []map[string]int{}
	for _, val := range m.mapper_files {
		file, _ := os.Open(val)
		var payload map[string]interface{}

		defer file.Close()
		table := make(map[string]int)

		decode := json.NewDecoder(file)

		for {
			if err := decode.Decode(&payload); err != nil {
				break
			}

			if letter, ok := payload["letter"].(string); ok {
				table[letter]++
			}
		}
		reducers = append(reducers, table)
	}

	// reduce
	m.mapper_groupt.Add(1)
	go Reduce(reducers, &m)
	m.mapper_groupt.Wait()

	for _, file := range m.mapper_files {
		e := os.Remove(file)

		if e != nil {
			log.Fatal("Couldn't remove file: ", e)
		}
	}
}
