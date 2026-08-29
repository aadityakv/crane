package shared

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// These are the arguments grep will always execute, in addition to the supplied args
const DefaultGrepArgs = "-n -H"

// Port to communicate with the grep function over
const GrepServerPort = 1234
const BOLT_PORT_INIT = 8320
const WORKER_PORT = "8260"
const TOP_PORT = "8120"
const SPOUT_PORT = "8122"
const ACK_PORT = "8124"
const SwimPingPort = 5678
const SwimACKPort = 5679
const SwimIntroducerPort = 5680
const FilePort = 5681

// If enabled, will output verbose grep loggings to a log file
const OutputGrepToLog = false

// Print out things related to failure detection
var PrintFailDetectInfo = false

// The number of servers we are using
const NumServers = 10
const FingerTableSize = 4
const NumFileReplicas = 4

// How long to wait to get the membership list from the introducer
const IntroducerTimeout = 3 * time.Second
const GrepTimeout = 5 * time.Second
const ACKTimeout = 1000 * time.Millisecond
const PingInterval = 1500 * time.Millisecond

// Simulate false positives by dropping packets before they are sent out
const FalsePosChance = 0.0

// Channel between membership list and replication logic
var GlobalMembershipChannel = make(chan MemLists, 5)

func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// Given the servNum, returns address in form of fa18-cs425-g27-{SERVNUM}.cs.illinois.edu
func GetServerAddressFromNumber(servNum int) (serverAddress string) {
	serverAddress = "fa18-cs425-g27-"
	if servNum != 10 {
		serverAddress += fmt.Sprintf("0%d%s", servNum, ".cs.illinois.edu")
	} else {
		serverAddress += fmt.Sprintf("%d%s", servNum, ".cs.illinois.edu")
	}
	return
}

// Returns a Log object that outputs logs to the specified file
func OpenLogFile(name string) *log.Logger {
	logFile, logFileErr := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if logFileErr != nil {
		log.Fatalf("error opening logfile: %v", logFileErr)
	}
	logger := log.New(logFile, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	return logger
}

func GetOwnServerNumber() (servNum int) {
	hostname, _ := exec.Command("hostname").Output()
	serverAddress := fmt.Sprintf("%s", hostname)
	servNum = GetServerNumberFromString(serverAddress)
	return
}

func GetServerNumberFromString(serverAddress string) (servNum int) {
	server := strings.Split(serverAddress, "fa18-cs425-g37-")
	if len(server) != 2 {
		panic("This is not one of the VMs, exiting execution.\n")
	}
	serverAddress = strings.Split(server[1], ".cs.illinois.edu")[0]
	if serverAddress[0] == '0' {
		servNum, _ = strconv.Atoi(string(serverAddress[1]))
	} else {
		servNum, _ = strconv.Atoi(serverAddress)
	}
	return
}

// ***************************************** //
// ************ RPC Definitions ************ //
// ***************************************** //

// ==== File System ==== //

type FileArgs struct {
	LocalFname, SdfsFname string
	FileContents          []byte
	NumVersions           int
}
type FileReply struct {
	OnMachine    bool
	FileContents []byte
}

type MemLists struct {
	OldList []bool
	NewList []bool
}

// ======= Grep ======= //

// arguments for grep
type GrepArgs_t struct {
	GrepArgs          []string
	FileGlob, Pattern string
	Verbose           bool
}

// return values for grep
type GrepReply_t struct {
	Out      string
	NumLines int
}
