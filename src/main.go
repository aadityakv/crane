package main

import (
	"bufio"
	"crane"
	"failure"
	"file_sys"
	"fmt"
	"grep_server"
	"log"
	"os"
	"os/exec"
	"shared"
	"strings"
	"topology"
)

const CMD_PROMPT = "> "

// crane_called := false

func main() {
	println("Starting server")
	log.SetFlags(log.Lshortfile)

	failure.Initialize()

	go func() {
		println("Type 'help' for list of commands")
		fmt.Printf("[Server %d]%s", shared.GetOwnServerNumber(), CMD_PROMPT)
		for {
			reader := bufio.NewReader(os.Stdin)
			cmd, _ := reader.ReadString('\n')
			go func() {
				HandleServerCommand(strings.TrimSuffix(cmd, "\n"))
				fmt.Printf("[Server %d]%s", shared.GetOwnServerNumber(), CMD_PROMPT)
			}()
		}
	}()

	file_sys.Initialize()
	grep_server.Initialize()

	if shared.GetOwnServerNumber() == 1 {
		th := crane.NewTaskHandler()
		go th.Run()
	} else {
		w := &crane.Worker{}
		go w.Run()
	}

	println("Finished starting server")
	// Keep the program running so it doesn't close the port
	for {
	}
}

func HandleServerCommand(cmd string) {
	switch com := strings.Split(cmd, " "); com[0] {
	case "":
		{
			println()
		}
	case "leave":
		{
			fmt.Println("Leaving group")
			failure.LeaveGroup()
			file_sys.Leave()
		}
	case "print_fail":
		{
			shared.PrintFailDetectInfo = !shared.PrintFailDetectInfo
		}
	case "memlist":
		{
			println(failure.MemList.Str(true))
		}
	case "clear":
		{
			cmd := exec.Command("clear")
			cmd.Stdout = os.Stdout
			cmd.Run()
		}
	// case "crane":
	// 	{
	// 		crane_called = true
	// 		go crane_node_controller()
	// 	}
	// case "crane-client":
	// 	{
	// 		go client(cmd)
	// 	}
	// case "master":
	// 	{
	// 		if crane_called == true {
	// 			go master()
	// 		} else {
	// 			fmt.Println("Crane must first be activated")
	// 		}
	// 	}
	case "put", "get", "delete", "ls", "store", "get-versions", "test":
		{
			fileCmdError := file_sys.HandleFileCmd(com[0], com[1:])
			if fileCmdError != nil {
				fmt.Printf("%v\n", fileCmdError)
			}
			println()
		}
	case "submit":
		{
			submitErr := topology.Submit(com[1])
			if submitErr != nil {
				fmt.Printf("%v\n", submitErr)
			}
		}
	case "help":
		{
			fmt.Printf("leave\nprint_fail\nmem_list\nput\nget\ndelete\nls\nstore\nget-versions\ncrane\nmaster\ncrane-client\n\n")
		}
	default:
		println("Invalid Command")
	}
}
