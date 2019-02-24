package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	ch := make(chan DoTaskArgs)
	go func() {
		for i := 0; i < ntasks; i++ {
			var fn string
			if phase == mapPhase {
				fn = mapFiles[i]
			} else {
				fn = ""
			}
			arg := DoTaskArgs{
				JobName:       jobName,
				File:          fn,
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: n_other,
			}
			ch <- arg
		}
		close(ch)
	}()
	// consume !!!!
	quit := make(chan int)
	go func() {
		for w := range registerChan {
			// go func(w string) {
			for {
				ar, ok := <-ch
				if !ok {
					quit <- 1
				}
				call(w, "Worker.DoTask", ar, nil)
			}
			// }(w)
		}

	}()
	<-quit

	fmt.Printf("Schedule: %v done\n", phase)
}

// for {
// 	select {
// 	case w := <-registerChan:
// 		ar, ok := <-ch
// 		if !ok {
// 			quit <- 1

// 		} else {
// 			call(w, "Worker.DoTask", ar, nil)
// 			fmt.Println("++++++++++++")

// 		}
// 	case <-quit:
// 		break
// 	}
// }
