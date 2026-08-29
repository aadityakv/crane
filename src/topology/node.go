package topology

import (
	"bufio"
	"encoding/gob"
	"errors"
	"file_sys"
	"fmt"
	"io/ioutil"
	"net"
	"plugin"
	// "log"
	"os"
	"strings"

	"shared"
)

type Node struct {
	FeatureType string
	SubType     string
	ID          string
	ParentID    string
	Tasks       int
	Grouping    string
}
type Tree struct {
	Name  string
	Nodes []Node
}

type TupAck struct {
	Tup    *Tuple
	Weight int
}
type Tuple struct {
	ID     uint32
	Fields map[string]string
}

type Destination struct {
	ID       string
	Tasks    int
	Grouping string
}
type HyperEdge struct {
	Root     string
	Weight   int
	Children []Destination
}

type Track struct {
	Children []string
	Weight   int
}
type WorkerTask struct {
	TaskID   string
	Hostname string
	Port     string
}

func NewBolt(subtype string, id string, parentID string, tasks int, grouping string) Node {
	bolt := Node{}
	bolt.FeatureType = "B"
	bolt.SubType = subtype
	bolt.ID = id
	bolt.ParentID = parentID
	bolt.Grouping = grouping
	bolt.Tasks = tasks

	return bolt
}

func NewSpout(subtype string, id string, tasks int) Node {
	spout := Node{}
	spout.FeatureType = "S"
	spout.SubType = subtype
	spout.ID = id
	spout.Tasks = tasks

	return spout
}

func (tree *Tree) AddSpout(node Node) {
	if node.FeatureType == "S" {
		tree.Nodes = append(tree.Nodes, node)
	}
}

func (tree *Tree) AddBolt(node Node) {
	if node.FeatureType == "B" {
		tree.Nodes = append(tree.Nodes, node)
	}
}

func Submit(filename string) error {
	file, err := os.Open(filename)

	if err != nil {
		return err
	}
	defer file.Close()
	topfile := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		cmd := scanner.Text()
		strs := strings.SplitN(cmd, " ", 2)
		if strs[1] != "topology.so" {
			content, err := ioutil.ReadFile(strs[0])
			if err != nil {
				return err
			}

			putArgs := shared.FileArgs{strs[0], file_sys.SDFS_Folder + file_sys.ReplaceSlashWithDivision(strs[1]), content, 0}
			err = file_sys.MakeRemoteCall("Put", putArgs)
			if err != nil {
				return err
			}
		} else {
			topfile = strs[0]
		}
	}

	return submitTopology(topfile)
}

func submitTopology(topfile string) error {
	p, err := plugin.Open(topfile)
	if err != nil {
		return err
	}

	symbol, err := p.Lookup("GetTree")
	if err != nil {
		return err
	}

	treefunc, ok := symbol.(func() Tree)
	if !ok {
		return errors.New("func fail")
	}

	tree := treefunc

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", shared.GetServerAddressFromNumber(1), shared.TOP_PORT))
	if err != nil {
		return err
	}
	defer conn.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	rw.WriteString("TOP\n")
	enc := gob.NewEncoder(conn)
	err = enc.Encode(tree)
	if err != nil {
		return err
	}
	err = rw.Flush()
	if err != nil {
		return err
	}

	return nil
}
