package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"encoding/json"

	"mvdan.cc/xurls"
)

// Messages should have a comment
// types brought to you by the lovely https://quicktype.io/
type Messages []Message

// UnmarshalMessages should have a comment
func UnmarshalMessages(data []byte) (Messages, error) {
	var r Messages
	err := json.Unmarshal(data, &r)
	return r, err
}

// Marshal should have a comment
func (r *Messages) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

// Message should have a comment
type Message struct {
	User        string       `json:"user"`
	Text        string       `json:"text"`
	Type        string       `json:"type"`
	Subtype     *string      `json:"subtype,omitempty"`
	Ts          string       `json:"ts"`
	Attachments []Attachment `json:"attachments"`
}

// Attachment should have a comment
type Attachment struct {
	ServiceName string `json:"service_name"`
	Title       string `json:"title"`
	TitleLink   string `json:"title_link"`
	Text        string `json:"text"`
	Fallback    string `json:"fallback"`
	ImageURL    string `json:"image_url"`
	FromURL     string `json:"from_url"`
	ImageWidth  int64  `json:"image_width"`
	ImageHeight int64  `json:"image_height"`
	ImageBytes  int64  `json:"image_bytes"`
	ServiceIcon string `json:"service_icon"`
	ID          int64  `json:"id"`
}

type kv struct {
	URL   string
	Value int
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func processDir(dir string) {
	return
}

func simplifyURL(url string) string {
	return strings.Replace(strings.Replace(url, "https://", "", -1), "http://", "", -1)
}
func processFile(filename string, counterChan chan string) {
	urlFinder := xurls.Strict()
	content, err := ioutil.ReadFile(filename)
	check(err)
	messages, err := UnmarshalMessages(content)
	check(err)
	for _, msg := range messages {
		urls := urlFinder.FindAllString(string(msg.Text), -1)
		if len(urls) > 0 {
			// fmt.Printf("%+v\n", urls)
			for _, url := range urls {
				splitURL := strings.Split(url, "|")
				url = splitURL[0]
				counterChan <- url
			}
		}
	}
	return
}

func main() {
	if len(os.Args) < 2 {
		fmt.Print("Not enough arguments sent.\n")
		fmt.Print("Try ./scrape-urls ~/Downloads/export\n")
		os.Exit(1)
	}
	dir := os.Args[1]
	fmt.Printf("Processing all .json files in %s", dir)
	subDirToSkip := "skip" // dir/to/walk/skip

	var fileWG, counterWG sync.WaitGroup
	counter := make(map[string]int)
	counterChan := make(chan string, 80000000)

	counterWG.Add(1)
	go func() {
		defer counterWG.Done()
		for url := range counterChan {
			counter[simplifyURL(url)]++
		}
	}()

	fileWG.Add(1)
	go func() {
		defer fileWG.Done()
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", dir, err)
				return err
			}
			if info.IsDir() && info.Name() == subDirToSkip {
				fmt.Printf("skipping a dir without errors: %+v \n", info.Name())
				return filepath.SkipDir
			}
			// fmt.Printf("visited file: %q\n", path)
			if !info.IsDir() && strings.HasSuffix(path, ".json") {
				fileWG.Add(1)
				go func() {
					defer fileWG.Done()
					processFile(path, counterChan)
				}()
			}
			return nil
		})

		if err != nil {
			fmt.Printf("error walking the path %q: %v\n", dir, err)
		}
	}()

	fileWG.Wait()
	close(counterChan)
	counterWG.Wait()

	var ss []kv
	for k, v := range counter {
		ss = append(ss, kv{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})

	for _, kv := range ss {
		fmt.Printf("%s: %d\n", kv.URL, kv.Value)
	}
}
