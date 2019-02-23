package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvs := map[string][]string{}

	for i := 0; i < nMap; i++ {
		fmt.Println(reduceName(jobName, i, reduceTask))
		filename := reduceName(jobName, i, reduceTask)
		f, _ := os.Open(filename)
		defer f.Close()
		dec := json.NewDecoder(f)
		kv := &KeyValue{}
		for {

			e := dec.Decode(kv)
			if e != nil {
				break
			}

			_, ok := kvs[kv.Key]
			if !ok {
				kvs[kv.Key] = []string{}
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)

			// enc.Encode(KeyValue{kv.Key, reduceF(kv.Key, []string{kv.Value})})
		}
	}

	// 取出所有key
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}

	// 对key 排序
	sort.Strings(keys)

	// 然后写进文件
	outF, _ := os.Create(outFile)
	defer outF.Close()
	enc := json.NewEncoder(outF)
	for _, k := range keys {
		enc.Encode(KeyValue{k, reduceF(k, kvs[k])})
	}
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
}
