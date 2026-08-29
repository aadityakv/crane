# ECE 428 - Distributed Systems

## Setup
* Add `export GOPATH=~/go/src/cs_425_mp3` to `~/.bashrc`, or modify existing, so local machine will be able to find packages.
* Run `make setup` so that Go will get all of the external packages, and it send the `tools/bashrc.txt` to every VM.

## How to Use
* Ansible method - run `clone_git_repos`, then `run_servers`
* TMux method
    * `./dev-tmux.sh`
    * <kbd>CTRL</kbd>+<kbd>B</kbd>, then <kbd>e</kbd>. This synchronizes the windows.
    * `cd ~/mp3/src`
    * `make`
    * `./ece428`

### Server Commands
Once the server has started on the VM, there are some commands you can run.
* `leave` - Makes the server gracefully exit the group
* `print_fail` - Toggles printing information regarding the failure detector
* `mem_list` - Prints the membership list
* `help` - Print out all the commands that can be run
* All of the file system commands

## TMux
tmux is a linux utility to open several terminal sessions in the same terminal window. Copy the `.tmux.conf` to `~/` to get the keyboard shortcuts. To run commands, type <kbd>CTRL</kbd>+<kbd>B</kbd>, then do the keyboard shortcut, or <kbd>:</kbd> to type a command. Type <kbd>ALT</kbd>+arrow key to change window.

## Measuring bandwidth

    sudo tcpdump -i eth0 -len port 5681 | ./bps.pl

## Extra Scripts
* `dev-tmux.sh` - This will open 10 ssh sessions in the same window using tmux.
* `send_to_all.sh /*USERNAME*/` - Sends the `src/` directory to every VM
* `send files.sh /*USERNAME*/ /*up to 4 VMs*/` - Sends the `src/` directory to up to 4 VMs, provide the number with leading zero.
* `send_bashrc.sh /*USERNAME*/` - Sends the `bashrc.txt` to every VM as it's `.bashrc`, so that the GOPATH will be correctly configured.
