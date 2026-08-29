package failure

import (
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"shared"
)

// Describes a server, may need to take out Hostname to reduce network bandwidth
type ServerInfo struct {
	Number   int
	Hostname string
}

// How individual entries in the membership list are identified
type MembershipId struct {
	TimeStamp time.Time
	ServNum   uint8
	Failed    bool
}

type MembershipEntry struct {
	Id          MembershipId
	Changed     bool
	ReceivedACK bool
	Mutex       sync.Mutex
}

type MembershipList struct {
	Servers       [shared.NumServers + 1]MembershipEntry
	UpdateFT      bool
	UpdateFTMutex sync.Mutex
}

type FingerTable struct {
	Entries [shared.FingerTableSize]ServerInfo
}

var Introducer = ServerInfo{1, shared.GetServerAddressFromNumber(1)}

var ownServerNum = shared.GetOwnServerNumber()
var MemList MembershipList
var fingerTable FingerTable
var fileLog = shared.OpenLogFile(fmt.Sprintf("detectFail%d.log", ownServerNum))
var green = color.New(color.FgGreen).SprintFunc()
var red = color.New(color.FgRed).SprintFunc()

const memListBufferSize int = 1024

func (servInf *ServerInfo) Str() string {
	return fmt.Sprintf("ServerIeD: %2d", servInf.Number)
}
func (mlId *MembershipId) Str() string {
	return fmt.Sprintf("Server Number: %d\nTimestamp: %v\nFailed: %v", mlId.ServNum, mlId.TimeStamp, mlId.Failed)
}
func (mlEntry *MembershipEntry) Str() (ret string) {
	mlEntry.Mutex.Lock()
	ret = fmt.Sprintf("%s\nChanged: %v\nReceivedACK: %v\n", mlEntry.Id.Str(), mlEntry.Changed, mlEntry.ReceivedACK)
	mlEntry.Mutex.Unlock()
	return
}
func (ml *MembershipList) Str(onlyPrintNums bool) (ret string) {
	for _, entry := range ml.Servers {
		if entry.Id.ServNum != 0 {
			if onlyPrintNums {
				if entry.Id.Failed {
					ret += fmt.Sprintf("%s, ", red(entry.Id.ServNum))
				} else {
					ret += fmt.Sprintf("%s, ", green(entry.Id.ServNum))
				}
			} else {
				ret += entry.Str() + "\n"
			}
		}
	}
	if ret == "" {
		ret = "Empty membership list\n"
	}
	return
}
func (ft *FingerTable) Str() (ret string) {
	for _, entry := range ft.Entries {
		if entry.Number != 0 {
			ret += entry.Str() + "\n\n"
		}
	}
	if ret == "" {
		ret = "Empty finger table\n"
	}
	return
}

// This function assumes you already have the lock to the entry
func (mlEntry *MembershipEntry) markFailed() {
	mlEntry.Id.Failed = true
	mlEntry.Changed = true
}

func (ml *MembershipList) GetChanged(returnAll bool) (changedML []MembershipId) {
	for i, entry := range ml.Servers {
		entry.Mutex.Lock()
		if entry.Changed == true || (returnAll && entry.Id.ServNum != 0 && !entry.Id.Failed) {
			changedML = append(changedML, entry.Id)
			ml.Servers[i].Changed = false
		}
		entry.Mutex.Unlock()
	}
	return
}

// Update the membership list
func (ml *MembershipList) Update(changedIds []MembershipId) {
	for _, newId := range changedIds {
		servNum := newId.ServNum
		update := false

		var memLists shared.MemLists
		for i := 0; i <= 10; i++ {
			MemList.Servers[i].Mutex.Lock()
			memLists.OldList = append(memLists.OldList, !(MemList.Servers[i].Id.Failed))
			MemList.Servers[i].Mutex.Unlock()
		}

		ml.Servers[servNum].Mutex.Lock()

		// Update if we don't have the server id
		// Update if new timestamp is later
		// Update if new entry says it failed but we thought it was alive
		if newId.ServNum != ml.Servers[servNum].Id.ServNum {
			fileLog.Printf("Server %d has joined the network\n", servNum)
			update = true
		} else if newId.TimeStamp.After(ml.Servers[servNum].Id.TimeStamp) {
			fileLog.Printf("Server %d has rejoined us!\n", servNum)
			update = true
		} else if newId.Failed == true && ml.Servers[servNum].Id.Failed == false {
			if int(newId.ServNum) == ownServerNum {
				fileLog.Printf("Everyone thinks I'm a failure\n")
				time.Sleep(500 * time.Millisecond)
				println("Restarting own process (not really right now")
				//Join()
			} else {
				fileLog.Printf("I received a message that server %d has failed\n", servNum)
				update = true
			}
		}

		// Update the entry
		if update == true {
			ml.Servers[servNum].Id = newId
			ml.Servers[servNum].Changed = true
		}

		ml.Servers[servNum].Mutex.Unlock()

		for i := 0; i <= 10; i++ {
			MemList.Servers[i].Mutex.Lock()
			memLists.NewList = append(memLists.NewList, !(MemList.Servers[i].Id.Failed))
			MemList.Servers[i].Mutex.Unlock()
		}

		shared.GlobalMembershipChannel <- memLists

		ml.UpdateFTMutex.Lock()
		ml.UpdateFT = true
		ml.UpdateFTMutex.Unlock()
	}
}

// Updates the finger table from the membership list
func (ft *FingerTable) Update() {
	// Set the bool to unchanged
	MemList.UpdateFTMutex.Lock()
	MemList.UpdateFT = false
	MemList.UpdateFTMutex.Unlock()

	self := ownServerNum
	powerOfTwo := 1

	addedServers := map[int]bool{}

	for i := 0; i < shared.FingerTableSize; i++ {
		// Find starting point of the search
		start := (self-1+powerOfTwo)%shared.NumServers + 1
		look := start

		// Set finger table entry to invalid
		ft.Entries[i].Number = 0
		ft.Entries[i].Hostname = ""

		// Find first server after start that hasn't already been added
		// 'cont' ensures that the loop is executed at least once
		for cont := true; cont || look != start; cont = false {
			// Check if server is online
			MemList.Servers[look].Mutex.Lock()
			isOnline := !MemList.Servers[look].Id.Failed && !MemList.Servers[look].Id.TimeStamp.IsZero()
			MemList.Servers[look].Mutex.Unlock()

			// Add server if not yourself, online, and not in another finger table entry
			if self != look && isOnline && !addedServers[look] {
				ft.Entries[i].Number = look
				ft.Entries[i].Hostname = shared.GetServerAddressFromNumber(look)

				addedServers[look] = true
				break
			}

			// Increment server number and take modulo
			look = (look % shared.NumServers) + 1
		}

		powerOfTwo *= 2
	}
}

func IamIntroducer() bool {
	return ownServerNum == Introducer.Number
}

func Join() {
	// Add yourself to your membership table
	servNum := ownServerNum
	MemList.Servers[servNum].Mutex.Lock()
	MemList.Servers[servNum].Id.ServNum = uint8(servNum)
	MemList.Servers[servNum].Id.TimeStamp = time.Now()
	MemList.Servers[servNum].Changed = true
	MemList.Servers[servNum].Mutex.Unlock()

	MemList.UpdateFTMutex.Lock()
	MemList.UpdateFT = true
	MemList.UpdateFTMutex.Unlock()

	fileLog.Printf("Server %d joining the network\n", servNum)

	if !IamIntroducer() {
		// Contact the introducer
		conn, pingErr := net.Dial("udp", fmt.Sprintf("%s:%d", Introducer.Hostname, shared.SwimIntroducerPort))
		if pingErr != nil {
			log.Panic("Introducer ping error: ", pingErr)
		}
		defer conn.Close()
		// Just ping the introducer, it will send the mem list back on the same connection
		conn.Write([]byte(""))
	} else {
		// Start ping interval for introducer
		go PingIntervalFunction()
	}
}

// Leave the group by
func LeaveGroup() {
	// Set your own membership entry to failed
	servNum := ownServerNum

	MemList.Servers[servNum].Mutex.Lock()
	MemList.Servers[servNum].markFailed()
	MemList.Servers[servNum].Mutex.Unlock()

	MemList.UpdateFTMutex.Lock()
	MemList.UpdateFT = true
	MemList.UpdateFTMutex.Unlock()

	// Ping your children
	SendPingAndMembershipList()

	// Stop your own process
	fileLog.Printf("Leaving the failure detector peacefully. Goodbye!")
	log.Println("Ending process normally")
	os.Exit(0)
}

func PingIntervalFunction() {
	for {
		SendPingAndMembershipList()
		time.Sleep(shared.PingInterval)
	}
}

// Send the membership list to children
func SendPingAndMembershipList() {
	// Update finger table only if membership list changed
	// TODO don't call finger tables every time
	//MemList.UpdateFTMutex.Lock()
	//hasChanged := MemList.UpdateFT
	//MemList.UpdateFTMutex.Unlock()
	//if hasChanged == true {
	//fingerTable.Update()
	//}
	fingerTable.Update()

	changedML := MemList.GetChanged(false)
	if shared.PrintFailDetectInfo {
		println("Mem list: " + MemList.Str(true))
	}
	if len(changedML) != 0 && shared.PrintFailDetectInfo {
		fmt.Printf("ChangedML is %v\n", changedML)
	}

	// pingTargets := fingerTable.Entries
	for i, server := range fingerTable.Entries {
		if server.Number == 0 {
			break
		}

		// Unmark this server as received an ACK from it
		MemList.Servers[server.Number].Mutex.Lock()
		MemList.Servers[server.Number].ReceivedACK = false
		MemList.Servers[server.Number].Mutex.Unlock()

		conn, pingErr := net.Dial("udp", fmt.Sprintf("%s:%d", server.Hostname, shared.SwimPingPort))
		if pingErr != nil {
			log.Panic("SendPingAndMembershipList ping error: ", pingErr)
		}
		defer conn.Close()

		changedMLJSON, _ := json.Marshal(changedML)

		conn.Write(changedMLJSON)
		// If the ACK doesn't come back in time, then mark as failed
		go func(ii int) {
			time.Sleep(shared.ACKTimeout)
			MemList.Servers[fingerTable.Entries[ii].Number].Mutex.Lock()
			if !MemList.Servers[fingerTable.Entries[ii].Number].ReceivedACK {
				MemList.Servers[fingerTable.Entries[ii].Number].markFailed()
				fileLog.Printf("I detected server %d has failed\n", fingerTable.Entries[ii].Number)
			}
			MemList.Servers[fingerTable.Entries[ii].Number].Mutex.Unlock()
		}(i)
	}
}

// Opens the port so that this machine can be pinged by others
func OpenPortForPing() {
	addr := net.UDPAddr{
		Port: shared.SwimPingPort,
	}
	conn, udpErr := net.ListenUDP("udp", &addr)
	if udpErr != nil {
		log.Panic("OpenPortForPing udp error: ", udpErr)
	}

	go func() {
		for {
			var buf [memListBufferSize]byte
			udpLen, senderAddr, readUDPErr := conn.ReadFromUDP(buf[:])
			if readUDPErr != nil {
				log.Panic("OpenPortForPing read udp error: ", readUDPErr)
			}
			// fmt.Printf("Received ping from %v\n", senderAddr)

			go func() {
				// Decode a struct sent over the network
				var memIds []MembershipId
				// println("Got a message on the ping port")
				_ = json.Unmarshal(buf[:udpLen], &memIds)
				if len(memIds) != 0 && shared.PrintFailDetectInfo {
					fmt.Printf("Message is %+v\n", memIds)
				}
				MemList.Update(memIds)
			}()
			go SendACK(conn, senderAddr)
		}
	}()
}

func SendACK(conn *net.UDPConn, addr *net.UDPAddr) {
	addr.Port = shared.SwimACKPort
	// TODO: Send failed to machine that is in current membership list as failed
	_, err := conn.WriteToUDP([]byte(strconv.Itoa(ownServerNum)), addr)
	if err != nil {
		fmt.Printf("Couldn't send ACK %v", err)
	}
}

// Opens the port so that this machine can receive acknowlegements from pings
func OpenPortForACK() {
	addr := net.UDPAddr{
		Port: shared.SwimACKPort,
	}
	conn, udpErr := net.ListenUDP("udp", &addr)
	if udpErr != nil {
		log.Panic("OpenPortForACK udp error: ", udpErr)
	}

	go func() {
		for {
			var buf [16]byte
			ackLen, _, readUDPErr := conn.ReadFromUDP(buf[:])
			if readUDPErr != nil {
				log.Panic("OpenPortForACK read udp error: ", readUDPErr)
			}

			go func() {
				str_buf := string(buf[:ackLen])
				if str_buf == "Failed" {
					MemList.Servers[fingerTable.Entries[ownServerNum].Number].Mutex.Lock()
					if !MemList.Servers[fingerTable.Entries[ownServerNum].Number].ReceivedACK {
						MemList.Servers[fingerTable.Entries[ownServerNum].Number].markFailed()
						fileLog.Printf("I detected server %d has failed\n", fingerTable.Entries[ownServerNum].Number)
					}
					MemList.Servers[fingerTable.Entries[ownServerNum].Number].Mutex.Unlock()
				}
				servNum, _ := strconv.Atoi(str_buf)
				if shared.PrintFailDetectInfo {
					fmt.Printf("Received ACK from server %d\n", servNum)
				}

				MemList.Servers[servNum].Mutex.Lock()
				MemList.Servers[servNum].ReceivedACK = true
				MemList.Servers[servNum].Mutex.Unlock()
			}()
		}
	}()
}

func OpenPortForIntroducer() {
	addr := net.UDPAddr{
		Port: shared.SwimIntroducerPort,
	}
	introducerCon, udpErr := net.ListenUDP("udp", &addr)
	if udpErr != nil {
		log.Panic("OpenPortForIntroducer ListenUDP error: ", udpErr)
	}

	if shared.PrintFailDetectInfo {
		println("Opening port for introducer")
	}
	go func() {
		for {
			var buf [memListBufferSize]byte
			if !IamIntroducer() {
				introducerCon.SetReadDeadline(time.Now().Add(shared.IntroducerTimeout))
			}
			udpLen, senderAddr, readUDPErr := introducerCon.ReadFromUDP(buf[:])
			if readUDPErr != nil {
				log.Panic("OpenPortForIntroducer ReadUDP error: ", readUDPErr)
			}

			if IamIntroducer() {
				if shared.PrintFailDetectInfo {
					println("I'm the introducer! I'll send my membership list to the pingy boi")
				}
				go func() {
					fullMemList := MemList.GetChanged(true)
					jsonList, _ := json.Marshal(fullMemList)
					introducerCon.Write(jsonList)
					senderAddr.Port = shared.SwimIntroducerPort
					_, _ = introducerCon.WriteToUDP(jsonList, senderAddr)
				}()
			} else {
				if shared.PrintFailDetectInfo {
					println("I'm the pingy boi, and I got the membership list from the introducer")
					fmt.Printf("Length of this message was %d bytes\n", udpLen)
				}
				go func() {
					var memIds []MembershipId
					_ = json.Unmarshal(buf[:udpLen], &memIds)
					MemList.Update(memIds)
					go PingIntervalFunction()
				}()
				// Close the connection, because the requester will never need to get the mem list again
				introducerCon.Close()
				return
			}
		}
	}()
}

func Initialize() {
	if shared.PrintFailDetectInfo {
		println("Initializing failure detector")
	}
	if !IamIntroducer() {
		time.Sleep(200 * time.Millisecond)
	}

	for i, _ := range MemList.Servers {
		if i != ownServerNum {
			MemList.Servers[i].Id.Failed = true
		}
	}

	OpenPortForPing()
	OpenPortForACK()
	OpenPortForIntroducer()
	if shared.PrintFailDetectInfo {
		println("Finished initializing failure detector")
	}

	if shared.PrintFailDetectInfo {
		println("Joining group")
	}
	Join()
	if shared.PrintFailDetectInfo {
		println("Successfully joined group")
	}
}
