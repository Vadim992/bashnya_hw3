package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Write code here

func ExecutePipeline(jobs ...job) {

	var wg sync.WaitGroup

	in := make(chan interface{})
	out := make(chan interface{})

	for i := 0; i < len(jobs); i++ {

		wg.Add(1)

		switch {

		case i == 0:

			go runPipeline(jobs[i], nil, out, &wg)

		case i%2 == 0:

			out = make(chan interface{})

			go runPipeline(jobs[i], in, out, &wg)

		default:

			in = make(chan interface{})

			go runPipeline(jobs[i], out, in, &wg)

		}
	}

	wg.Wait()
}

func runPipeline(worker job, in, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	worker(in, out)
	close(out)
}

func runGoroutine(ch chan<- string, data string, job func(string) string, wg *sync.WaitGroup) {
	defer wg.Done()
	ch <- job(data)
}

func runGoroutineLock(ch chan<- string, data string, job func(string) string, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done()
	mu.Lock()
	ch <- job(data)
	mu.Unlock()
}

func SingleHash(in, out chan interface{}) {

	var wgSingle sync.WaitGroup
	var mu sync.Mutex

	for val := range in {

		strChan1 := make(chan string)
		strChan2 := make(chan string)

		data := strconv.Itoa(val.(int))

		wgSingle.Add(4)

		go runGoroutineLock(strChan2, data, DataSignerMd5, &wgSingle, &mu)
		go runGoroutine(strChan1, data, DataSignerCrc32, &wgSingle)
		go runGoroutine(strChan2, <-strChan2, DataSignerCrc32, &wgSingle)

		go func(out chan interface{}, ch1, ch2 <-chan string, wg *sync.WaitGroup) {
			defer wg.Done()
			out <- (<-ch1 + "~" + <-ch2)
		}(out, strChan1, strChan2, &wgSingle)

	}

	wgSingle.Wait()
}

func MultiHash(in, out chan interface{}) {
	var wgMulti sync.WaitGroup

	s := make([][]string, 0, MaxInputDataLen)

	for val := range in {

		data := val.(string)
		arr := make([]string, 6)

		s = append(s, arr)

		for j := 0; j < 6; j++ {

			wgMulti.Add(1)

			str := fmt.Sprintf("%d%s", j, data)

			go func(arr []string, ind int, data string, work func(string) string, wg *sync.WaitGroup) {
				defer wg.Done()
				arr[ind] = work(data)
			}(arr, j, str, DataSignerCrc32, &wgMulti)

		}
	}
	wgMulti.Wait()

	out <- s
}

func CombineResults(in, out chan interface{}) {

	val := <-in

	data := val.([][]string)

	s := make([]string, 0, len(data))

	max := 0

	for _, elem := range data {
		str := strings.Join(elem, "")
		if len(str) > max {
			max = len(str)
		}
		s = append(s, str)
	}

	sort.Slice(s, func(i, j int) bool {

		str1, str2 := s[i], s[j]

		if len(str1) < max {
			num := max - len(str1)
			str1 += strings.Repeat("0", num)
		}

		if len(str2) < max {
			num := max - len(str2)
			str2 += strings.Repeat("0", num)
		}

		for i := range str1 {
			num1, _ := strconv.Atoi(string(str1[i]))
			num2, _ := strconv.Atoi(string(str2[i]))
			if num1 != num2 {
				return num1 < num2
			}
		}

		return false
	})

	out <- strings.Join(s, "_")
}
