package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {

	// fmt.Println("jobname:", jobName, "maptask:", mapTask, "inflie:", inFile, "except2:", nReduce)
	//  jobname: test maptask: 0 inflie: 824-mrinput-0.txt except2: 2

	f, _ := os.OpenFile(inFile, os.O_RDONLY, 6)
	defer f.Close()
	bs, _ := ioutil.ReadAll(f)
	kvs := mapF(inFile, string(bs))
	interfiles := []*os.File{}

	for i := 0; i < nReduce; i++ {
		filename := reduceName(jobName, mapTask, i)
		// fmt.Println(filename)
		interfile, _ := os.Create(filename)
		defer interfile.Close()
		interfiles = append(interfiles, interfile)
		// fmt.Println(interfile)

	}

	for _, kv := range kvs {

		i := ihash(kv.Key) % nReduce
		file := interfiles[i]
		enc := json.NewEncoder(file)
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println(err)
		}
	}

	// mapF(inFile)

	//  *****
	// doMap manages one map task: it should read one of the input files
	// (inFile), call the user-defined map function (mapF) for that file's
	// contents, and partition mapF's output into nReduce intermediate files.
	//
	// There is one intermediate file per reduce task. The file name
	// includes both the map task number and the reduce task number. Use
	// the filename generated by reduceName(jobName, mapTask, r)
	// as the intermediate file for reduce task r. Call ihash() (see
	// below) on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at Go's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//
	// Your code here (Part I).
	//
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
