package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// My_Data data structure
type mydata struct {
	Text   string
	Number int
	Value  float64
}

// DataManager struct to manage data insertion, removal
type DataManager struct {
	data        []mydata
	insert      chan mydata
	remove      chan chan mydata
	done        chan struct{}
	fullSignal  chan struct{}
	emptySignal chan struct{}
	maxSize     int
}

// ResultManager struct to collect and store results
type ResultManager struct {
	results []mydata
	insert  chan mydata
	collect chan []mydata
	done    chan struct{}
}

// Run function for DataManager
func (dm *DataManager) Run() {
	for {
		select {
		case item := <-dm.insert:

			if len(dm.data) >= dm.maxSize {
				select {
				case dm.emptySignal <- struct{}{}:

				default:
				}
			}
			dm.data = append(dm.data, item)
			if len(dm.data) == dm.maxSize {
				select {
				case dm.fullSignal <- struct{}{}:

				default:
				}
			}
		case req := <-dm.remove:
			if len(dm.data) > 0 {
				req <- dm.data[0]

				dm.data = dm.data[1:]
			} else {

				close(req)
			}
		case <-dm.done:

			close(dm.insert)
			close(dm.remove)
			return
		}
	}
}

// Worker function
func Worker(dataRequest chan<- chan mydata, resultChan chan<- mydata, doneChan <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-doneChan:

			return
		default:
			reqChan := make(chan mydata)
			dataRequest <- reqChan
			data, ok := <-reqChan
			if !ok {

				return
			}

			// Only send data with even numbers to the result channel
			if data.Number%2 == 0 {

				resultChan <- data
			}
		}
	}
}

// Run function for ResultManager
func (rm *ResultManager) Run() {
	for {
		select {
		case result := <-rm.insert:

			rm.results = append(rm.results, result)
		case <-rm.done:

			sort.Slice(rm.results, func(i, j int) bool {
				return rm.results[i].Number < rm.results[j].Number
			})
			rm.collect <- rm.results
			return
		}
	}
}

// Read data from file
func read(dataFile string, dataManager *DataManager) (int, error) {
	file, err := os.Open(dataFile)
	if err != nil {
		return 0, fmt.Errorf("Error reading file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ";")
		if len(parts) >= 3 {
			text := parts[0]
			number, _ := strconv.Atoi(parts[1])
			value, _ := strconv.ParseFloat(parts[2], 64)
			dataManager.insert <- mydata{text, number, value}
			count++
		}
	}
	return count, nil
}

// Write results to file
func Print(resultFile string, results []mydata) error {
	output, err := os.Create(resultFile)
	if err != nil {
		return fmt.Errorf("Error creating result file: %v", err)
	}
	defer output.Close()

	writer := bufio.NewWriter(output)
	writer.WriteString("| Text | Number | Value |\n")

	for _, data := range results {
		// Skip entries with empty text or zero values
		if data.Text != "" && (data.Number != 0 || data.Value != 0) {
			writer.WriteString(fmt.Sprintf("| %s | %d | %.2f |\n", data.Text, data.Number, data.Value))
		}
	}

	return writer.Flush()
}

func main() {
	dataFile := "C:\\Users\\manon\\OneDrive\\Desktop\\mano_data1.txt"
	resultFile := "Result_data.txt"
	resultChan := make(chan mydata)
	dataRequest := make(chan chan mydata)
	doneChan := make(chan struct{})
	collectChan := make(chan []mydata)
	var wg sync.WaitGroup

	// Initialize DataManager and ResultManager
	dataManager := DataManager{
		data:        make([]mydata, 0, 10),
		insert:      make(chan mydata),
		remove:      dataRequest,
		done:        make(chan struct{}),
		fullSignal:  make(chan struct{}, 1),
		emptySignal: make(chan struct{}, 1),
	}
	resultManager := ResultManager{
		results: make([]mydata, 0),
		insert:  resultChan,
		collect: collectChan,
		done:    doneChan,
	}

	// Starting data manager and result manager as goroutines
	go dataManager.Run()
	go resultManager.Run()

	// Load data using readData function
	fmt.Println("Loading data from file", dataFile)
	count, err := read(dataFile, &dataManager)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	dataManager.maxSize = count / 2

	// Start worker processes
	numWorkers := 6

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go Worker(dataRequest, resultChan, doneChan, &wg)
	}

	// Wait for all workers to finish

	wg.Wait()

	// Close the result channel and signal completion
	close(resultChan)
	doneChan <- struct{}{}

	// Collect and write final results using writeResults function
	results := <-collectChan
	if err := Print(resultFile, results); err != nil {
		fmt.Println("Error writing results:", err)
		return
	}

	fmt.Println("Main: Processing completed. Results saved in", resultFile)
}
