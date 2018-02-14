package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

type byKey []KeyValue

func (s byKey) Len() int {
	return len(s)
}
func (s byKey) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s byKey) Less(i, j int) bool {
	return s[i].Key < s[j].Key
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	kvs := make([]KeyValue, 0)

	for i := 0; i < nMap; i++ {
		reduceFileName := reduceName(jobName, i, reduceTask)
		file, reduceReadErr := os.Open(reduceFileName)

		if reduceReadErr != nil {
			log.Fatal("Reduce error while reading: ", reduceReadErr)
		}

		defer file.Close()

		dec := json.NewDecoder(file)

		var kv KeyValue

		for dec.More() {

			decodeErr := dec.Decode(&kv)

			if decodeErr != nil {
				log.Fatal("Reduce error while decoding: ", decodeErr)
			}

			kvs = append(kvs, kv)

		}

	}

	sort.Sort(byKey(kvs))

	keyValueMap := make(map[string][]string)

	for _, kv := range kvs {
		_, ok := keyValueMap[kv.Key]

		if ok {
			keyValueMap[kv.Key] = append(keyValueMap[kv.Key], kv.Value)
		} else {
			keyValueMap[kv.Key] = make([]string, 0)
			keyValueMap[kv.Key] = append(keyValueMap[kv.Key], kv.Value)
		}

	}

	mergedFileName := mergeName(jobName, reduceTask)

	mergedFile, writeErr := os.Create(mergedFileName)

	if writeErr != nil {
		log.Fatal("Reduce error while writing: ", writeErr)
	}

	defer mergedFile.Close()

	encoder := json.NewEncoder(mergedFile)

	for k, v := range keyValueMap {

		encoder.Encode(KeyValue{k, reduceF(k, v)})

	}

}
