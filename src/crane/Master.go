package crane

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"file_sys"
	"log"
	"math/rand"
	"os"
	"plugin"
	"sync"
	"time"
	// "bufio"
	"failure"
	"topology"
	// "file_sys"
	"fmt"
	// "grep_server"
	// "log"
	// "os"
	// "os/exec"
	"net"
	"shared"
	"strconv"
)

type TaskHandler struct {
	WorkerTasks [shared.NumServers + 1][]string
	TaskHolders map[string]topology.WorkerTask
	TaskEdge    map[string]topology.HyperEdge
}

type AckTrack struct {
	Counts map[uint32]int
	Mutex  sync.Mutex
}

type SpoutData struct {
	Expiry time.Time
	Tup    topology.Tuple
}

var buf bytes.Buffer

func NewTaskHandler() *TaskHandler {
	th := TaskHandler{}
	th.TaskHolders = make(map[string]topology.WorkerTask)
	th.TaskEdge = make(map[string]topology.HyperEdge)
	return &th
}

func (th *TaskHandler) Run() {
	listen, err := net.Listen("tcp", ":"+shared.TOP_PORT)

	if err != nil {
		fmt.Errorf("workrun%s\n", err)
		return
	}

	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Errorf("worklisten%s\n", err)
			continue
		}
		go th.Handle(conn)
	}
}

func (th *TaskHandler) Handle(conn net.Conn) {
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	defer conn.Close()
	m, err := rw.ReadString('\n')
	if err != nil {
		fmt.Errorf("Read error%s\n", err)
	}
	dec := gob.NewDecoder(conn)
	if m == "TOP" {
		tree := topology.Tree{}
		err := dec.Decode(&tree)
		if err != nil {
			fmt.Errorf("handlehe error %s\n", err)
			return
		}
		go th.RunTree(tree)
	}

	err = rw.Flush()
	if err != nil {
		fmt.Errorf("flush error %s\n", err)
	}
}

func (th *TaskHandler) RunTree(tree topology.Tree) {
	he := th.SendTasks(tree)

	th.SendUpdates()

	go th.RunSpout(tree.Name, he)
	// run spout on HE

}

func (acker *AckTrack) Count(jobname, listenPort string) {
	intport, err := strconv.Atoi(listenPort)
	if err != nil {
		fmt.Errorf("atoi error%s\n", err)
		return
	}

	outFile, err := os.OpenFile(jobname+".txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Errorf("outfile error %s\n", err)
		return
	}

	outlog := log.New(outFile, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: intport})

	if err != nil {
		fmt.Errorf("listen err%s\n", err)
	}

	defer conn.Close()
	buf := make([]byte, 4096)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			fmt.Errorf("Read error %s\n", err)
			return
		}
		tupac := topology.TupAck{}
		tupac.Tup.Fields = make(map[string]string)
		err = gob.NewDecoder(bytes.NewReader(buf[:n])).Decode(&tupac)
		if err != nil {
			fmt.Errorf("Tuple decode error %s\n", err)
			return
		}
		acker.Counts[tupac.Tup.ID] += tupac.Weight

		if tupac.Weight == 1 {
			outlog.Printf("%s|| Tuple %d - ", time.Now().String(), tupac.Tup.ID)

			for k, v := range tupac.Tup.Fields {
				outlog.Printf("%s: %s;", k, v)
			}
			outlog.Printf("\n")
		}
	}

}
func (th *TaskHandler) RunSpout(jobname string, he topology.HyperEdge) {
	ack := &AckTrack{}
	ack.Counts = make(map[uint32]int)

	tuples := make([]SpoutData, 0)
	go ack.Count(jobname, shared.ACK_PORT)
	var emptyByteArray []byte
	getArgs := shared.FileArgs{he.Root + ".so", file_sys.SDFS_Folder + file_sys.ReplaceSlashWithDivision(he.Root+".so"), emptyByteArray, 0}
	err := file_sys.MakeRemoteCall("Get", getArgs)
	if err != nil {
		fmt.Errorf("runspouterror%s\n", err)
		return
	}
	p, err := plugin.Open(he.Root + ".so")
	if err != nil {
		fmt.Errorf("plugin open error %s\n", err)
		return
	}

	symbol, err := p.Lookup("Execute")
	if err != nil {
		fmt.Errorf("func open error %s\n", err)
		return
	}

	spout, ok := symbol.(func(uint32) (*topology.Tuple, bool))
	if !ok {
		fmt.Println("Func failure")
		return
	}
	id := uint32(0)
	for {
		sd := SpoutData{}
		sd.Tup.Fields = make(map[string]string)
		if len(tuples) >= 1000 {
			sd.Expiry = time.Now().Add(time.Second * 5)

			curr := tuples[0]
			if curr.Expiry.After(time.Now()) {
				go func() {
					ack.Mutex.Lock()
					i := 0
					for {
						if i >= len(tuples) {
							break
						}
						val, ok := ack.Counts[tuples[i].Tup.ID]

						if !ok || val >= he.Weight {
							tuples = append(tuples[:i], tuples[i+1:]...)
						} else {
							i = i + 1
						}
					}
					ack.Mutex.Unlock()
				}()
				continue
			}
			sd.Tup.ID = curr.Tup.ID
			tuples = tuples[1:]
			a, ok := ack.Counts[sd.Tup.ID]
			if !ok {
				continue
			}
			if a >= he.Weight {
				delete(ack.Counts, sd.Tup.ID)
				continue
			}

			for k, v := range curr.Tup.Fields {
				sd.Tup.Fields[k] = v
			}

			// go func() {
			// 	for i, data := range tuples {
			// 		val, ok := ack.Counts[data.Tup.ID]

			// 		if i < len(tuples) - 1 {
			// 			if !ok || val >= he.Weight{
			// 				tuples = append(tuples[:i], tupes[i+1:]...)
			// 			}
			// 		}
			// 	}
			// }()

		} else {
			target, done := spout(id)
			if done {
				break
			}
			id = id + 1
			sd.Tup.ID = target.ID

			for k, v := range target.Fields {
				sd.Tup.Fields[k] = v
			}
			sd.Expiry = time.Now().Add(time.Second * 5)
		}
		tuples = append(tuples, sd)
		th.sendForward(&sd.Tup, he)
	}
}

func (th *TaskHandler) sendForward(tuple *topology.Tuple, he topology.HyperEdge) {
	for _, dest := range he.Children {
		if dest.Grouping != "" {
			val, ok := tuple.Fields[dest.Grouping]

			if !ok {
				fmt.Printf("No field %s in tuple\n", dest.Grouping)
			} else {
				h := shared.Hash(val) % uint32(dest.Tasks)
				key := dest.ID + "-" + strconv.Itoa(int(h))
				wt, work := th.TaskHolders[key]

				if !work {
					fmt.Printf("No worker task present for %s\n", key)
					continue
				}

				conn, err := net.Dial("udp", fmt.Sprintf("%s:%s", wt.Hostname, wt.Port))

				if err != nil {
					fmt.Errorf("startup conn fail %s\n", err)
					continue
				}
				buf.Reset()
				err = gob.NewEncoder(&buf).Encode(*tuple)

				if err != nil {
					fmt.Errorf("Encode fail %s\n", err)
					conn.Close()
					continue
				}

				conn.Write(buf.Bytes())
				conn.Close()
			}
		} else {
			h := rand.Int() % dest.Tasks
			key := dest.ID + "-" + strconv.Itoa(h)
			wt, work := th.TaskHolders[key]

			if !work {
				fmt.Printf("No worker task present for %s\n", key)
				continue
			}

			conn, err := net.Dial("udp", fmt.Sprintf("%s:%s", wt.Hostname, wt.Port))

			if err != nil {
				fmt.Errorf("startup conn fail %s\n", err)
				continue
			}

			buf.Reset()
			err = gob.NewEncoder(&buf).Encode(*tuple)

			if err != nil {
				fmt.Errorf("Encode fail %s\n", err)
				conn.Close()
				continue
			}

			conn.Write(buf.Bytes())
			conn.Close()
		}
	}
}

func GetNodeMap(tree topology.Tree) map[string]topology.Node {
	nodemap := make(map[string]topology.Node)

	for _, node := range tree.Nodes {
		nodemap[node.ID] = node
	}

	return nodemap
}

func GetSpoutID(tree topology.Tree) string {
	for _, node := range tree.Nodes {
		if node.FeatureType == "S" {
			return node.ID
		}
	}
	return ""
}

func GetWeight(id string, dependents map[string]topology.Track) int {
	track, v := dependents[id]

	if v {
		for _, child := range track.Children {
			track.Weight += GetWeight(child, dependents)
		}
		return track.Weight
	} else {
		dependents[id] = topology.Track{Weight: 1}
		return 1
	}
}
func HostPort(hostname, port string) string {
	return hostname + ":" + port
}

func GetDependents(tree topology.Tree) map[string]topology.Track {
	dependents := make(map[string]topology.Track)

	for _, node := range tree.Nodes {
		if node.ParentID != "" {
			_, v := dependents[node.ParentID]

			if !v {
				dependents[node.ParentID] = topology.Track{Weight: 0}
			}
			track, _ := dependents[node.ParentID]
			track.Children = append(track.Children, node.ID)
		}
	}

	return dependents
}

func (th *TaskHandler) SendTaskHelper(subtype string, worker int, key string, he topology.HyperEdge) int {
	conn, err := net.Dial("tcp", HostPort(shared.GetServerAddressFromNumber(worker), shared.WORKER_PORT))
	if err != nil {
		fmt.Errorf("sth error%s\n", err)
		return -1
	}
	defer conn.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	rw.WriteString("DIRECT\n")
	rw.WriteString(subtype + "\n")
	enc := gob.NewEncoder(conn)
	err = enc.Encode(he)
	if err != nil {
		fmt.Errorf("sth errorenc%s\n", err)
		return -1
	}
	err = rw.Flush()
	if err != nil {
		fmt.Errorf("sth errorflush%s\n", err)
		return -1
	}
	resp, err := rw.ReadString('\n')
	if err != nil {
		fmt.Errorf("sth errorresp%s\n", err)
		return -1
	}

	ret, err := strconv.Atoi(resp)
	return ret
}

func (th *TaskHandler) SendTask(node topology.Node, he topology.HyperEdge) {
	for i := 0; i < node.Tasks; i++ {
		r := 1
		key := node.ID + "-" + strconv.Itoa(i)
		port := -1
		for {
			r = rand.Int() % (shared.NumServers + 1)
			if r != 1 && failure.MemList.Servers[r].Id.Failed == false {
				port = th.SendTaskHelper(node.SubType, r, key, he)
				if port != -1 {
					break
				}
			}
		}
		th.TaskHolders[key] = topology.WorkerTask{node.SubType, shared.GetServerAddressFromNumber(r), strconv.Itoa(port)}
		th.TaskEdge[key] = he
		th.WorkerTasks[r] = append(th.WorkerTasks[r], key)

	}
}

func (th *TaskHandler) HandleFailures() {
	flag := false

	for i, entry := range failure.MemList.Servers {
		if entry.Id.Failed == false {
			continue
		}
		flag = true
		for _, task := range th.WorkerTasks[i] {
			r := 1
			port := -1
			he := th.TaskEdge[task]
			wt, ok := th.TaskHolders[task]
			st := ""
			if ok {
				st = wt.TaskID
			}

			for {
				r = rand.Int() % (shared.NumServers + 1)
				if r != 1 && failure.MemList.Servers[r].Id.Failed == false {
					port = th.SendTaskHelper(st, r, task, he)
					if port != -1 {
						break
					}
				}
			}
			th.TaskHolders[task] = topology.WorkerTask{st, shared.GetServerAddressFromNumber(r), strconv.Itoa(port)}
			th.WorkerTasks[r] = append(th.WorkerTasks[r], task)
		}
		th.WorkerTasks[i] = nil
	}

	if flag {
		th.SendUpdates()
	}
}
func (th *TaskHandler) SendUpdates() {
	for i, entry := range failure.MemList.Servers {
		if entry.Id.Failed {
			continue
		}
		conn, err := net.Dial("tcp", HostPort(shared.GetServerAddressFromNumber(i), shared.WORKER_PORT))
		if err != nil {
			continue
		}
		rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		rw.WriteString("UPDATE\n")
		enc := gob.NewEncoder(conn)
		err = enc.Encode(th.TaskHolders)
		if err != nil {
			conn.Close()
			continue
		}
		err = rw.Flush()
		if err != nil {
			fmt.Errorf("sth errorflush%s\n", err)
			conn.Close()
			continue
		}
		conn.Close()
	}
}
func (th *TaskHandler) SendTasks(tree topology.Tree) topology.HyperEdge {
	nodemap := GetNodeMap(tree)
	dependents := GetDependents(tree)
	spout := GetSpoutID(tree)

	GetWeight(spout, dependents)

	for _, node := range tree.Nodes {
		if node.ID == spout {
			continue
		}
		he := topology.HyperEdge{Root: spout, Weight: dependents[node.ID].Weight}
		for _, child := range dependents[node.ID].Children {
			he.Children = append(he.Children, topology.Destination{ID: child, Tasks: nodemap[child].Tasks, Grouping: nodemap[child].Grouping})
		}
		th.SendTask(nodemap[node.ID], he)
	}
	he := topology.HyperEdge{Root: spout, Weight: dependents[spout].Weight}
	for _, child := range dependents[spout].Children {
		he.Children = append(he.Children, topology.Destination{ID: child, Tasks: nodemap[child].Tasks, Grouping: nodemap[child].Grouping})
	}
	th.TaskEdge[spout] = he
	th.TaskHolders[spout] = topology.WorkerTask{spout, shared.GetServerAddressFromNumber(1), shared.SPOUT_PORT}

	return he
}

type CraneNode struct {
	Number   int
	Hostname string
	MainM    bool
	StandbyM bool
	MainS    bool
	Sup_1    bool
	Sup_2    bool
}

type CraneTable struct {
	Entries []CraneNode
}

// master := false
// standby_master := false
// servNum_of_master := -1000000
// servNum_of_standby_master := -1000000
// crane_server_port := 1800
// master_crane_server_port := 1900
// supervisor_crane_server_port := 2000
// topology_server_port := 2100

var crane_membership_list CraneTable

// func master() {
// 	//server for master for incoming connections
// 	master_notify()
// 	standby_choose_and_notify()
// while 1==1 {
// check if standby failed if main
// check if main failed if standby
// check for incoming client requests
// if so then start up 3 supervisors and convey them the job info
// send standby master job info
// call job handler till job is completed
// }

// func master_notify(){
// 	master = true
// 	servNum_of_master = shared.GetOwnServerNumber()
// 	for i, server := range MembershipList.Servers {
// 		if server.ID.ServNum != servNum_of_master {
// 			conn, pingErr := net.Dial("udp", fmt.Sprintf("%s:%d", GetServerAddressFromNumber(server.ID.ServNum), crane_server_port))
// 			if pingErr != nil {
// 				log.Panic("Master ping error: ", pingErr)
// 			}
// 			conn.Write([]byte("Master:"+ string(servNum_of_master)))
// 			buffer := make([]byte, 1024)
// 			_ := conn.SetReadDeadline(time.Now().Add(time.Millisecond*300))
//     	    _, remoteAddr, _ = conn.ReadFromUDP(buffer)
//     	    if strings.Contains(string(buffer),"ack-master") {
// 				crane_membership_list.Entries = append(crane_membership_list.Entries, CraneNode{i, remoteAddr.Str(), false, false, false, false, false})
//     	    }
// 			// if read ack from machine's crane server, then its part of the cluster
// 				// add it to the cluster membership_list
// 			conn.Close()
// 		}
// 	}
// }
//
// func standby_choose_and_notify(){
// 	rand.Seed(time.Now().Unix())
// 	while standby_master ==false {
// 		Index_of_potential_standby_master := rand.Int() % len(crane_membership_list.Entries)
// 		service := GetServerAddressFromNumber(crane_membership_list.Entries[Index_of_potential_standby_master].Number) + ":" + strconv.Itoi(crane_server_port)
// 		conn, pingErr := net.Dial("udp", service)
// 		if pingErr != nil {
// 			log.Panic("Master ping error: ", pingErr)
// 		}
// 		conn.Write([]byte("YouAreStandbyMaster:"))
// 		buffer := make([]byte, 1024)
// 		_ := conn.SetReadDeadline(time.Now().Add(time.Millisecond*300))
// 		_, remoteAddr, _ = conn.ReadFromUDP(buffer)
// 		if strings.Contains(string(buffer),"IAmStandbyMaster") {
// 			crane_membership_list.Entries[Index_of_potential_standby_master].StandbyM = true
// 			standby_master = true
// 			servNum_of_standby_master = Index_of_potential_standby_master
// 			//send standby-master crane-cluster list
// 		}
// 		conn.Close()
// 	}
// 	for i, server := range crane_membership_list.Entries {
// 		// notify all others in crane-cluster of standby master
// 	}
// }
//
// func main_sup_choose_and_notify(){
//
// }
//
// func sup_1_choose_and_notify(){
//
// }
//
// func sup_2_choose_and_notify(){
//
// }
//
// func crane_controller(){
// 	// deal with incoming Master (Main, standby), Supervisor (Main, sup_1, sup_2), and Worker messages
// }
//
// func supervisor_controller(){
//
// }
