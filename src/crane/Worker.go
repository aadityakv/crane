package crane

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"file_sys"
	"math/rand"
	"net"
	"fmt"
	"plugin"
	"shared"
	"strconv"
	"sync"
	// "bufio"
	// "failure"
	// "file_sys"
	"topology"
	// "grep_server"
	// "log"
	// "os"
	// "os/exec"
	// "shared"
	// "strings"
)

type Worker struct {
	TaskHolders map[string]topology.WorkerTask
	NextPort    int
	Mutex       sync.Mutex
}

var workbuf bytes.Buffer
var workLog = shared.OpenLogFile(fmt.Sprintf("worker.log"))
func (w *Worker) Run() error {
	w.TaskHolders = make(map[string]topology.WorkerTask)
	w.NextPort = shared.BOLT_PORT_INIT
	listen, err := net.Listen("tcp", ":"+shared.WORKER_PORT)
	if err != nil {
		workLog.Printf("workrun%s\n", err)
		return err
	}

	for {
		conn, err := listen.Accept()
		if err != nil {
			workLog.Printf("worklisten%s\n", err)
			return err
		}
		go w.Handle(conn)
	}

}

func (w *Worker) RunBolt(port string, subtype string, he topology.HyperEdge) {
	var emptyByteArray []byte
	getArgs := shared.FileArgs{subtype + ".so", file_sys.SDFS_Folder + file_sys.ReplaceSlashWithDivision(subtype+".so"), emptyByteArray, 0}
	err := file_sys.MakeRemoteCall("Get", getArgs)
	if err != nil {
		workLog.Printf("runbolterror%s\n", err)
		return
	}
	p, err := plugin.Open(subtype + ".so")
	if err != nil {
		workLog.Printf("plugin open error %s\n", err)
		return
	}

	symbol, err := p.Lookup("Execute")
	if err != nil {
		workLog.Printf("func open error %s\n", err)
		return
	}

	bolt, ok := symbol.(func(*topology.Tuple) (*topology.Tuple, bool))
	if !ok {
		workLog.Println("Func failure")
		return
	}

	intport, err := strconv.Atoi(port)
	if err != nil {
		workLog.Printf("atoi error%s\n", err)
		return
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: intport})
	if err != nil {
		workLog.Printf("UDP bolt error%s\n", err)
		return
	}

	defer conn.Close()
	buf := make([]byte, 4096)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			workLog.Printf("Read error %s\n", err)
			return
		}
		tuple := topology.Tuple{}
		tuple.Fields = make(map[string]string)
		err = gob.NewDecoder(bytes.NewReader(buf[:n])).Decode(&tuple)
		if err != nil {
			workLog.Printf("Tuple decode error %s\n", err)
			return
		}

		t, ok := bolt(&tuple)
		if !ok || len(he.Children) == 0 {
			go w.acknowledge(t, he.Root, he.Weight)
		} else {
			go w.sendForward(t, he)
		}
	}
}

func (w *Worker) sendForward(tuple *topology.Tuple, he topology.HyperEdge) {
	for _, dest := range he.Children {
		if dest.Grouping != "" {
			val, ok := tuple.Fields[dest.Grouping]

			if !ok {
				workLog.Printf("No field %s in tuple\n", dest.Grouping)
			} else {
				h := int(shared.Hash(val) % uint32(dest.Tasks))
				key := dest.ID + "-" + strconv.Itoa(h)
				wt, work := w.TaskHolders[key]

				if !work {
					workLog.Printf("No worker task present for %s\n", key)
					continue
				}

				conn, err := net.Dial("udp", fmt.Sprintf("%s:%s", wt.Hostname, wt.Port))

				if err != nil {
					workLog.Printf("startup conn fail %s\n", err)
					continue
				}
				workbuf.Reset()
				err = gob.NewEncoder(&workbuf).Encode(*tuple)

				if err != nil {
					workLog.Printf("Encode fail %s\n", err)
					conn.Close()
					continue
				}

				conn.Write(workbuf.Bytes())
				conn.Close()
			}
		} else {
			h := rand.Int() % dest.Tasks
			key := dest.ID + "-" + strconv.Itoa(h)
			wt, work := w.TaskHolders[key]

			if !work {
				workLog.Printf("No worker task present for %s\n", key)
				continue
			}

			conn, err := net.Dial("udp", fmt.Sprintf("%s:%s", wt.Hostname, wt.Port))

			if err != nil {
				workLog.Printf("startup conn fail %s\n", err)
				continue
			}
			workbuf.Reset()
			err = gob.NewEncoder(&workbuf).Encode(*tuple)

			if err != nil {
				workLog.Printf("Encode fail %s\n", err)
				conn.Close()
				continue
			}

			conn.Write(workbuf.Bytes())
			conn.Close()
		}
	}
}

func (w *Worker) acknowledge(tuple *topology.Tuple, spout string, weight int) {
	wt, ok := w.TaskHolders[spout]

	if !ok {
		return
	}
	conn, err := net.Dial("udp", fmt.Sprintf("%s:%s", wt.Hostname, wt.Port))

	if err != nil {
		workLog.Printf("Ack fail %s\n", err)
		return
	}
	defer conn.Close()

	tupac := topology.TupAck{Tup: tuple, Weight: weight}
	workbuf.Reset()
	err = gob.NewEncoder(&workbuf).Encode(tupac)

	if err != nil {
		workLog.Printf("Encode fail %s\n", err)
		return
	}

	conn.Write(workbuf.Bytes())
	conn.Write([]byte(strconv.Itoa(weight) + "\n"))
}

func (w *Worker) Handle(conn net.Conn) {
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	defer conn.Close()
	m, err := rw.ReadString('\n')
	if err != nil {
		workLog.Printf("rs error %s\n", err)
		return
	}
	dec := gob.NewDecoder(conn)
	if m == "DIRECT" {
		port := 0
		w.Mutex.Lock()
		port = w.NextPort
		w.NextPort += 1
		w.Mutex.Unlock()
		st, err := rw.ReadString('\n')
		if err != nil {
			workLog.Printf("rs error %s\n", err)
			return
		}
		he := topology.HyperEdge{}
		err = dec.Decode(&he)
		if err != nil {
			workLog.Printf("handlehe error %s\n", err)
			return
		}
		rw.WriteString(strconv.Itoa(port) + "\n")
		go w.RunBolt(strconv.Itoa(port), st, he)
	} else if m == "UPDATE" {
		w.TaskHolders = make(map[string]topology.WorkerTask)
		err := dec.Decode(&w.TaskHolders)
		if err != nil {
			workLog.Printf("handle map erorr %s\n", err)
			return
		}
	}
	err = rw.Flush()
	if err != nil {
		workLog.Printf("flush error %s\n", err)
	}

}

func Spout() {
	// Read file of key value tuples from SDFS
	// Establish TCP client to each of the targets
	// While no failure and while not end-of-file:
	// 		Read line of file
	// Emit line to each of the targets using TCP which has a back pressure mechanism built in
	// Notify Master of end of data file and send end-data-stream message to each of the targets
}

func Sink() {
	// Establish TCP server
	// While not received a end-of-stream message:
	// 		Send key-value pair to SDFS
	// If end-of-stream notify supervisors
}

func Bolt() {
	// Establish TCP server
	// While not received a end-of-stream message:
	// 		Call user-defined function ie script_sc1
}
func Filter() {
	// Create a TCP client to each of the targets
	// Apply defined_function to the list of key_value pairs
	// Send result tuple by tuple to each target
}

func Join() { //not sure
	// Create a TCP client to each of the targets
	// Apply defined_function to the list of key_value pairs
	// Send result tuple by tuple to each target
}

func Transform() {
	// Create a TCP client to each of the targets
	// Apply defined_function to the list of key_value pairs
	// Send result tuple by tuple to each target
}
