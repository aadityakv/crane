package grep_server

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"shared"
)

type GrepLogger int

// The function definitition for grep that complies with the rpc rules
// The client calls this on each server
func (t *GrepLogger) Grep(args *shared.GrepArgs_t, reply *shared.GrepReply_t) error {
	return GrepHelp(args.GrepArgs, args.FileGlob, args.Pattern, args.Verbose, reply)
}

// The implementation of grep
// Needed for the test cases
func GrepHelp(grepArgs []string, fileGlob, pattern string, verbose bool, reply *shared.GrepReply_t) error {
	grepLogObj := shared.OpenLogFile("machineLog.log")

	// Get the files, interpreting the file string as a shell glob
	files, globErr := filepath.Glob(fileGlob)
	if globErr != nil {
		return errors.New(fmt.Sprintf("%s: %v\n", globErr.Error(), fileGlob))
	} else if len(files) == 0 {
		return errors.New("no log files found")
	}

	grepArgs = append(append(strings.Fields(shared.DefaultGrepArgs), grepArgs...), pattern)
	grepArgs = append(grepArgs, files...)
	if verbose {
		fmt.Printf("grepArgs = %#v\n", grepArgs)
		println("command: grep " + strings.Join(grepArgs[:], " "))
	}
	if shared.OutputGrepToLog {
		grepLogObj.Printf("grepArgs = %#v\n", grepArgs)
		grepLogObj.Println("command: grep " + strings.Join(grepArgs[:], " "))
	}

	// Execute the local grep command
	cmd := exec.Command("grep", grepArgs...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	grepErr := cmd.Run()

	// Error checking
	if grepErr != nil {
		exiterr, _ := grepErr.(*exec.ExitError)
		// If grep exits with error code 1, then no output was found
		if status, _ := exiterr.Sys().(syscall.WaitStatus); status.ExitStatus() != 1 {
			return errors.New(fmt.Sprintf("%s: %v\n", grepErr.Error(), stderr.String()))
		} else if verbose {
			println("No matching patterns")
		}
	}

	grepOutput := stdout.String()
	outputLen := 0
	for _, el := range strings.Split(grepOutput, "\n") {
		if len(el) != 0 {
			outputLen++
		}
	}

	if verbose {
		println(grepOutput)
		fmt.Printf("Number of lines matched: %v\n", outputLen)
	}
	if shared.OutputGrepToLog {
		grepLogObj.Printf("Number of lines matched: %v\n", outputLen)
	}

	// Populate the response to the client
	reply.Out = stdout.String()
	reply.NumLines = outputLen
	return nil
}

func openGrepPortForRPC() {
	grepLogger := new(GrepLogger)
	rpc.Register(grepLogger)
	listener, lisErr := net.Listen("tcp", fmt.Sprintf(":%d", shared.GrepServerPort))
	if lisErr != nil {
		log.Fatal("grep listen error: ", lisErr)
	}
	for {
    conn, err := listener.Accept()
    if err != nil {
			log.Fatal("grep listen error: ", lisErr)
			return
    }

    go rpc.ServeConn(conn)
  }
}

func Initialize() {
	openGrepPortForRPC()
}
