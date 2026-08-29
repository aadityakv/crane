package file_sys

import (
	"crypto/sha256"
	"encoding/binary"
  "fmt"
	"failure"
	"log"
	"net"
	"net/rpc"
	"os"
	"shared"
	"time"
)

type RemoteFile int

var memList = failure.MemList

func (t *RemoteFile) Put(args *shared.FileArgs, reply *shared.FileReply) error {
	return PutFile(args.LocalFname, args.SdfsFname, args.FileContents)
}

func (t *RemoteFile) Get(args *shared.FileArgs, reply *shared.FileReply) error {
	data, e := GetFile(args.SdfsFname, args.LocalFname)
	reply.FileContents = data
	return e
}

func (t *RemoteFile) Delete(args *shared.FileArgs, reply *shared.FileReply) error {
	onMachine, err := DeleteFile(args.SdfsFname)
	reply.OnMachine = onMachine
	return err
}

func (t *RemoteFile) LS(args *shared.FileArgs, reply *shared.FileReply) error {
	onMachine, err := LSFile(args.SdfsFname)
	reply.OnMachine = onMachine
	return err
}

func (t *RemoteFile) GetVersions(args *shared.FileArgs, reply *shared.FileReply) error {
	data, e := GetVersions(args.SdfsFname, args.NumVersions, args.LocalFname)
	reply.FileContents = data
	return e
}

func (t *RemoteFile) SendFile(args *shared.FileArgs, reply *shared.FileReply) error {
	return ReceiveFile(args.SdfsFname, args.FileContents)
}

func GetMachinesHoldingFileFromMemList(sdfsFname string, memlist []bool) (replicas []bool) {
	replicas = make([]bool, shared.NumServers+1)
	hash := sha256.Sum256([]byte(sdfsFname))
	hashint := binary.BigEndian.Uint64(hash[:])

	startingMachine := (hashint % shared.NumServers) + 1
	// fmt.Printf("First machine for %s is: %v\n", sdfsFname, startingMachine)

	curMachine := int(startingMachine)
	curNumReplicas := 0
	for {
		if curMachine < len(memlist) && memlist[curMachine] {
			replicas[curMachine] = true
			curNumReplicas += 1
			// fmt.Printf("Machine %v holding %v\n", curMachine, sdfsFname)
		}

		curMachine = (curMachine % shared.NumServers) + 1
		if curMachine == int(startingMachine) || curNumReplicas == shared.NumFileReplicas { break }
	}
	return
}

func GetMachinesHoldingFile(sdfsFname string) (replicas []string) {
	hash := sha256.Sum256([]byte(sdfsFname))
	hashint := binary.BigEndian.Uint64(hash[:])

	startingMachine := (hashint % shared.NumServers) + 1
	fmt.Printf("First machine for %s is: %v\n", sdfsFname, startingMachine)

	curMachine := int(startingMachine)
	curNumReplicas := 0
	for {
		failure.MemList.Servers[curMachine].Mutex.Lock()
		if failure.MemList.Servers[curMachine].Id.Failed == false {
			replicas = append(replicas, shared.GetServerAddressFromNumber(curMachine))
			curNumReplicas += 1
			fmt.Printf("Machine %v holding %v\n", curMachine, sdfsFname)
		}
		failure.MemList.Servers[curMachine].Mutex.Unlock()

		curMachine = (curMachine % shared.NumServers) + 1
		if curMachine == int(startingMachine) || curNumReplicas == shared.NumFileReplicas { break }
	}
	return
}

// remoteFunction needs to be the name of one of the functions above
func MakeRemoteCall(remoteFunction string, remoteArgs shared.FileArgs) (err error) {
	start := time.Now()
	switch remoteFunction {
	case "Put":
		err = RemotePut(remoteFunction, remoteArgs)
	case "Get":
		err = RemoteGetAndGetVersions(remoteFunction, remoteArgs)
	case "Delete":
		err = RemoteDeleteAndLS(remoteFunction, remoteArgs)
	case "LS":
		err = RemoteDeleteAndLS(remoteFunction, remoteArgs)
	case "GetVersions":
		err = RemoteGetAndGetVersions(remoteFunction, remoteArgs)
	case "default":
		err = fmt.Errorf("Unknown function call to MakeRemoteCall: %s", remoteFunction)
	}

	elapsed := time.Since(start)
	fmt.Printf("%s took %s", remoteFunction, elapsed)

	return
}

func RemotePut(remoteFunction string, remoteArgs shared.FileArgs) (error) {
	replicas := GetMachinesHoldingFile(remoteArgs.SdfsFname)
	var calls = make([]*rpc.Call, len(replicas))
	var replies = make([]*rpc.Call, len(replicas))

	// Call the remote servers
	for index, address := range replicas {
		conn, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", address, shared.FilePort))

		// The server is unable to be reached
		if err != nil {
			calls[index] = nil
		} else {
			var reply shared.FileReply
			calls[index] = conn.Go("RemoteFile." + remoteFunction, &remoteArgs, &reply, nil)
		}
	}

	// Collect the responses
	responses := 0
	for index, _ := range replicas {
		if calls[index] == nil {
			continue
		}
		replies[index] = <-calls[index].Done

		// Check for server errors
		if replies[index].Error != nil {
			//fmt.Printf("Write failed on server %2d\n", GetServerNumberFromString(hostname))
			return fmt.Errorf("%s: Remote error on server %2d: %v\n", remoteFunction, index, replies[index].Error)
		} else {
			responses++
		}
	}

	if responses >= 4 {
		fmt.Printf("Write successful\n")
	} else {
		return fmt.Errorf("Only wrote to %d replicas\n", responses)
	}
	return nil
}

func RemoteGetAndGetVersions(remoteFunction string, remoteArgs shared.FileArgs) (error) {
	replicas := GetMachinesHoldingFile(remoteArgs.SdfsFname)
	var calls = make([]*rpc.Call, len(replicas))
	var replies = make([]*rpc.Call, len(replicas))

	// Call the remote servers
	for index, address := range replicas {
		conn, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", address, shared.FilePort))

		// The server is unable to be reached
		if err != nil {
			calls[index] = nil
		} else {
			var reply shared.FileReply
			calls[index] = conn.Go("RemoteFile." + remoteFunction, &remoteArgs, &reply, nil)
		}
	}

	// var getChan chan string = make(chan string)

	// Collect the responses
	// TODO : return when first response (in time) is collected
	// OR at least remove this comment so we don't draw attention to it
	for index, _ := range replicas {
		if calls[index] == nil {
			continue
		}
		replies[index] = <-calls[index].Done
		if replies[index].Error == nil {
			// Delete/clear local file if it already exists
			os.Remove(remoteArgs.LocalFname)

			// Write to the local file
			localF, err := os.OpenFile(remoteArgs.LocalFname, os.O_RDWR|os.O_CREATE, 0600)
			if err != nil {
				return fmt.Errorf("File error: %s\n", err)
			}
			defer localF.Close()

			contents := replies[index].Reply.(*shared.FileReply).FileContents
			_, writeErr := localF.Write(contents)
			if writeErr != nil {
        return writeErr
      }
      fileSysLog.Printf("Wrote contents of %s to local file %s", remoteArgs.SdfsFname, remoteArgs.LocalFname)
			return nil
		}
	}
	return fmt.Errorf("Get failed, no servers responded\n")
}

func RemoteDeleteAndLS(remoteFunction string, remoteArgs shared.FileArgs) (error) {
	const NumServs = shared.NumServers
	var calls [NumServs+1]*rpc.Call
	var replies [NumServs+1]*rpc.Call

	// Call the remote servers
	for i := 1; i <= NumServs; i++ {
		serverAddress := shared.GetServerAddressFromNumber(i)
		conn, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", serverAddress, shared.FilePort))

		// The server is unable to be reached, but that doesn't matter, because that machine doesn't have the file
		if err != nil {
			calls[i] = nil
		} else {
			var reply shared.FileReply
			calls[i] = conn.Go("RemoteFile." + remoteFunction, &remoteArgs, &reply, nil)
		}
	}

	// Collect the responses
	for i := 1; i <= NumServs; i++ {
		if calls[i] == nil {
			continue
		}
		replies[i] = <-calls[i].Done
		onMachine := replies[i].Reply.(*shared.FileReply).OnMachine

		// Check for server errors
		if replies[i].Error != nil {
			return fmt.Errorf("%sRemote error on server %2d: %v\n", remoteFunction, i, replies[i].Error)
		}
		if onMachine {
			fmt.Printf("Server %2d\n", i)
		}
	}
	return nil
}

func RemoteSendFile(sdfsFname string, contents []byte, server int) (error) {
	hostname := shared.GetServerAddressFromNumber(server)
	remoteArgs := shared.FileArgs{"", sdfsFname, contents, 0}

	conn, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", hostname, shared.FilePort))

	if err != nil {
		return err
	}

	var reply shared.FileReply
	call := conn.Go("RemoteFile.SendFile", &remoteArgs, &reply, nil)

	result := <-call.Done
	if result.Error != nil {
		return fmt.Errorf("Background replication to server %2d failed: %v\n", server, result.Error)
	}

	return nil
}

func openFilePortForRPC() {
	remoteFile := new(RemoteFile)
	server := rpc.NewServer()
	server.Register(remoteFile)
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", shared.FilePort))
	if e != nil {
		log.Fatal("Error trying to listen for RemoteFile RPC calls: ", e)
	}
	server.Accept(l)
}

func openFilePortForRPCInGoRoutine() {
	remoteFile := new(RemoteFile)
	rpc.Register(remoteFile)
	listener, lisErr := net.Listen("tcp", fmt.Sprintf(":%d", shared.FilePort))
	if lisErr != nil {
		log.Fatal("Error trying to listen for RemoteFile RPC calls: ", lisErr)
	}
	for {
    conn, err := listener.Accept()
    if err != nil {
			log.Fatal("Error trying to listen for RemoteFile RPC calls: ", lisErr)
			return
    }

    go rpc.ServeConn(conn)
  }
}
