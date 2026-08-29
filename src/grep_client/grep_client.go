package main

import (
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"shared"
	"strings"
)

const NumServs = shared.NumServers

// TODO: add a timeout for the server
func main() {
	// Parse the command line arguments
	grepArgsPtr := flag.String("grep", "", "The list of flags to pass to grep. "+shared.DefaultGrepArgs+" always used.")
	fileGlobPtr := flag.String("input_file", "*.log", "Glob to match files against. Wrap in quotes to use wildcards")
	patternPtr := flag.String("pattern", "", "Search criteria. Wrap in quotes to use wildcards")
	verbosePtr := flag.Bool("verbose", false, "Print debugging info")
	outputFilePtr := flag.String("output_file", "", "Name of the file to output to")
	flag.Parse()

	// Open an output file
	var outfile *os.File
	var err error
	if len(*outputFilePtr) != 0 {
		outfile, err = os.Create(*outputFilePtr)
		if err != nil {
			panic(err)
		}
	}
	defer outfile.Close()

	var calls [NumServs]*rpc.Call
	var replies [NumServs]*rpc.Call
	var lengths [NumServs]int

	// Call the remote servers
	for i := 1; i <= NumServs; i++ {
		serverAddress := shared.GetServerAddressFromNumber(i)

		conn, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", serverAddress, shared.GrepServerPort))

		// Call the server if connection was successful
		if err != nil {
			fmt.Printf("DIAL ERROR on server %2d: %v\n", i, err)
			lengths[i-1] = -1
		} else {
			args := &shared.GrepArgs_t{strings.Fields(*grepArgsPtr), *fileGlobPtr, *patternPtr, *verbosePtr}
			var reply shared.GrepReply_t
			calls[i-1] = conn.Go("GrepLogger.Grep", args, &reply, nil)
		}
	}

	// Collect the responses
	for i := 1; i <= NumServs; i++ {
		if lengths[i-1] >= 0 {
			replies[i-1] = <-calls[i-1].Done

			// Check for server errors
			if replies[i-1].Error != nil {
				fmt.Printf("GREP ERROR on server %2d: %v\n", i, replies[i-1].Error)
				lengths[i-1] = -1
			}

			// Print the lines and record counts
			if lengths[i-1] >= 0 {
				lengths[i-1] = replies[i-1].Reply.(*shared.GrepReply_t).NumLines
				fmt.Print(replies[i-1].Reply.(*shared.GrepReply_t).Out)
			}
		}
	}

	// Print line counts
	var total = 0
	for i := 1; i <= NumServs; i++ {
		if lengths[i-1] >= 0 {
			total += lengths[i-1]
			fmt.Printf("Server %2d has %6d lines.\n", i, lengths[i-1])
		}
	}
	fmt.Printf("The total number of lines is %d.\n", total)
}
