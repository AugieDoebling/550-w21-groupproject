package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

const nodeCount = 8

// raft node states
const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// communication reply flags
const (
	Ack     = 0
	VoteYes = 1
	VoteNo  = 2
)

const (
	Create = 0
	Delete = 1
	Write  = 2
	Chmod  = 3
)

type FileServerNode struct {
	id               int
	election_timeout int
	status           int
	term             int
	leader_id        int
	timeout_ticker   time.Ticker
	received_hb      bool
	logs             []EntryLog

	dataDirectory string
	changeIgnore  map[string]bool
}

type EntryLog struct {
	Id        int
	FileName  string
	FilePath  string
	EventType int
	NodeID    int
	IpAddress string

	Term  int
	Index int
}

type Heartbeat struct {
	Term      int
	Leader_id int
}

// rpc listener for vote request
// takes term of candidate requesting votes and returns VoteNo or VoteYes
func (n *FileServerNode) RequestVote(term_num int, reply *int) error {
	if term_num > n.term {
		*reply = VoteYes
		n.term = term_num
	} else {
		*reply = VoteNo
	}
	return nil
}

// Call an election
func AnnounceCandidacy(n *FileServerNode) {
	// 1. Increment term and switch to candidate
	n.term++
	n.status = Candidate

	println("announcing candidacy")

	// 2. request votes from all other nodes
	voteCount := 0
	nodeResponses := nodeCount

	for nid := 0; nid < nodeCount; nid++ {
		// Rather than sending RPC call to yourself, just vote for yourself.
		if nid == n.id {
			voteCount++
			continue
		}

		client, httpError := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:900%d", nid))
		if httpError != nil {
			println(nid, " - offline")
			// println(" - node",  down")
			nodeResponses -= 1
			continue
		}
		res := VoteNo
		clientErr := client.Call("FileServerNode.RequestVote", n.term, &res)
		if clientErr != nil {
			fmt.Println("Error: FileServerNode.RequestVote()", clientErr)
			continue
		}

		println(nid, "voted", res)

		if res == VoteYes {
			voteCount++
		}
	}

	println("received", voteCount, "votes")

	// 3. If you win the election, become a leader
	if voteCount > (nodeResponses / 2.0) {
		n.status = Leader
		println("i am the leader - term", n.term)
	}
}

// rpc listener for leader heartbeat
// takes id of heartbeating leader and responds with ack
func (n *FileServerNode) LeaderHeartbeat(heartbeat *Heartbeat, reply *int) error {
	if n.term <= heartbeat.Term {
		n.leader_id = heartbeat.Leader_id
		n.status = Follower
		n.term = heartbeat.Term
		// mark that we've received a heartbeat this timeout cycle
		n.received_hb = true
		// println("received heartbeat from leader", heartbeat.Leader_id, heartbeat.Term)
	}

	*reply = Ack
	return nil
}

func (n *FileServerNode) AppendEntries(logs []EntryLog, reply *int) error {
	latestNodeLog := n.logs[len(n.logs)-1]
	if (latestNodeLog.Index + 1) == logs[0].Index {
		n.logs = append(n.logs, logs...)
		println("Recieved:", latestNodeLog.Index, latestNodeLog.Term)
		println("Log size:", len(n.logs))
	} else {
		return rpc.ServerError("Inconsistency found, resend logs")
	}
	*reply = Ack
	return nil
}

func WatchTimer(node *FileServerNode) {
	for {
		select {
		case <-node.timeout_ticker.C:
			if (!node.received_hb) && node.status == Follower {
				AnnounceCandidacy(node)
			}
			node.received_hb = false
		}
	}
}

func sendHeartbeat(n *FileServerNode, sendToNodeId int) {
	client, httpError := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:900%d", sendToNodeId))
	if httpError != nil {
		// println(" - node down")
		return
	}
	res := 0

	hb := new(Heartbeat)
	hb.Term = n.term
	hb.Leader_id = n.id

	log := new(EntryLog)
	log.Index = (n.logs[len(n.logs)-1].Index + 1)
	log.Term = n.term

	clientErr := client.Call("FileServerNode.LeaderHeartbeat", hb, &res)
	if clientErr != nil {
		fmt.Println("Error: FileServerNode.LeaderHeartbeat", clientErr)
		return
	}
	n.logs = append(n.logs, *log)

	println("Leader: ", n.logs[len(n.logs)-1].Index, n.logs[len(n.logs)-1].Term)
	rectificationLog := []EntryLog{}
	entriesErr := client.Call("FileServerNode.AppendEntries", n.logs, &res)
	if entriesErr != nil {
		rectificationLog = append(rectificationLog, *log)
		for i := len(n.logs) - 1; i >= 0; i-- {
			sendErr := client.Call("FileServerNode.AppendEntries", rectificationLog, &res)
			if sendErr == nil {
				break
			}
			rectificationLog = append([]EntryLog{n.logs[i]}, rectificationLog...)
		}
	}
}

func sendHeartbeatWhenLeader(node *FileServerNode) {
	for range time.Tick(50 * time.Millisecond) {
		if node.status == Leader {
			// print("sending heartbeats (term ", node.term, ") : ")
			for i := 0; i < 8; i++ {
				if i != node.id {
					// print(i)
					go sendHeartbeat(node, i)
				}
			}
			// println()
		}
	}
}

type FileMessage struct {
	FileName string
	Data     []byte
}

func (node *FileServerNode) ReceiveCreateFile(file FileMessage, reply *int) error {
	println("received file - name", file.FileName, "data - ", string(file.Data))
	*reply = 1

	// os.Wr
	filePath := fmt.Sprintf("%s%c%s", node.dataDirectory, os.PathSeparator, file.FileName)

	ignoreNextChange(node, filePath)

	writeErr := os.WriteFile(filePath, file.Data, 0644)
	if writeErr != nil {
		*reply = 0
		println("Unable to write to file", filePath, writeErr)
	}

	return nil

}

func sendFile(node *FileServerNode, fileName string) {

	var readErr error
	var message = FileMessage{
		FileName: getRelativeFileName(node, fileName),
	}

	// read file contents
	message.Data, readErr = os.ReadFile(fileName)
	if readErr != nil {
		println("UNABLE TO READ FILE", fileName)
		return
	}

	client, httpError := rpc.DialHTTP("tcp", "localhost:9002")
	if httpError != nil {
		println("could not communicate with node - ")
		return
	}
	res := 0

	receiveErr := client.Call("FileServerNode.ReceiveCreateFile", message, &res)
	if receiveErr != nil || res != 1 {
		println("error while sending file")
		return
	}
}

func getRelativeFileName(node *FileServerNode, filename string) string {
	return strings.Replace(filename, node.dataDirectory+"/", "", 1)
}

func (node *FileServerNode) ReceiveDeleteFile(fileName string, reply *int) error {
	println("deleting file at", fileName)

	*reply = 1

	// os.Wr
	filePath := fmt.Sprintf("%s%c%s", node.dataDirectory, os.PathSeparator, fileName)

	ignoreNextChange(node, filePath)

	deleteErr := os.Remove(filePath)
	if deleteErr != nil {
		*reply = 0
		println("Unable to delete file", filePath, deleteErr.Error())
	}

	return nil
}

func deleteFile(node *FileServerNode, fileName string) {

	relativeFileName := getRelativeFileName(node, fileName)

	client, httpError := rpc.DialHTTP("tcp", "localhost:9002")
	if httpError != nil {
		println("could not communicate with node - ")
		return
	}
	res := 0

	receiveErr := client.Call("FileServerNode.ReceiveDeleteFile", relativeFileName, &res)
	if receiveErr != nil || res != 1 {
		println("error while deleting file")
		return
	}

}

func (node *FileServerNode) ReceiveCreateDirectory(dir string, reply *int) error {
	println("creating directory at", dir)

	*reply = 1

	// os.Wr
	dirPath := fmt.Sprintf("%s%c%s", node.dataDirectory, os.PathSeparator, dir)

	ignoreNextChange(node, dirPath)

	mkdirErr := os.Mkdir(dirPath, 0755)
	// deleteErr := os.Remove(filePath)
	if mkdirErr != nil {
		*reply = 0
		println("Unable to create directory", dirPath, mkdirErr.Error())
	}

	return nil
}

func createDirectory(node *FileServerNode, dir string) {
	relativeDirName := getRelativeFileName(node, dir)

	client, httpError := rpc.DialHTTP("tcp", "localhost:9002")
	if httpError != nil {
		println("could not communicate with node - ")
		return
	}
	res := 0

	receiveErr := client.Call("FileServerNode.ReceiveCreateDirectory", relativeDirName, &res)
	if receiveErr != nil || res != 1 {
		println("error while creating directory")
		return
	}

}

func ignoreNextChange(node *FileServerNode, filePath string) {
	node.changeIgnore[filePath] = true
}

func ignoreExists(node *FileServerNode, filePath string) bool {
	ignore, present := node.changeIgnore[filePath]

	if ignore {
		node.changeIgnore[filePath] = false
	}

	return ignore && present
}

// each node watches for changes and notifies leader when relevant change occurs
// leader writes the change to the log

// todo
// leader change listener - augie
// finish watcher - nicks
// get ip address for the pi's and store that to the logs - nicks
// method to call leader change (writes the log) from the node with a change
// node get changes listener - augie
// node get changes client - with file transfer - augie

// todo continuing / strech
// file transfer encryption
// chmod command
// merge conflicts
// file server capability - load balancer out of the leader

func watchForChanges(node *FileServerNode) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				// log.Println("event:", event)

				// ignore all filename starting with .
				if strings.HasPrefix(event.Name, ".") {
					println("ignoring event for file", event.Name)
					continue
				}

				// if not chmod
				if event.Op&fsnotify.Chmod != fsnotify.Chmod {
					if ignoreExists(node, event.Name) {
						println("found ignore for ", event.Name)
						continue
					}
					println("did not find ignore for", event.Name)

				}

				if event.Op&fsnotify.Write == fsnotify.Write {
					// log.Println("modified file:", event.Name)
					println("*** WRITE -", event.Name)
					// We add RPC functions to commit file change logs to leader
					sendFile(node, event.Name)
				}
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					println("*** DELETE -", event.Name)
					deleteFile(node, event.Name)
					// We need to check if the path is outside working directory
					// fileStat, err := os.Stat(event.Name)
					// if err != nil {
					// 	log.Println(err)
					// }

				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					println("*** CREATE -", event.Name)
					fileStat, err := os.Stat(event.Name)
					if err != nil {
						log.Println(err)
					}

					// fmt.Println("File Name:", fileStat.Name())        // Base name of the file
					// fmt.Println("Size:", fileStat.Size())             // Length in bytes for regular files
					// fmt.Println("Permissions:", fileStat.Mode())      // File mode bits
					// fmt.Println("Last Modified:", fileStat.ModTime()) // Last modification time

					fmt.Println("Is Directory: ", fileStat.IsDir())
					if fileStat.IsDir() {
						watcher.Add(event.Name)
						createDirectory(node, event.Name)
					} else {
						sendFile(node, event.Name)
					}

					// We add RPC functions to commit file change logs to leader
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	err = watcher.Add(node.dataDirectory)
	if err != nil {
		log.Fatal(err)
	}
	<-done
}

func createNode(listenPort int, dataDirectory string) {
	node := new(FileServerNode)
	node.status = Follower
	// set the leader as uninitialized
	node.leader_id = -1
	// set the election timeout
	node.election_timeout = rand.Intn(3000-1500) + 1500
	// start the timer
	node.timeout_ticker = *time.NewTicker(time.Second)
	// initialize empty logs
	node.logs = []EntryLog{
		{
			Term:  0,
			Index: 0,
		},
	}

	node.changeIgnore = make(map[string]bool)

	node.dataDirectory = dataDirectory

	// go WatchTimer(node)
	// go sendHeartbeatWhenLeader(node)
	go watchForChanges(node)

	rpc.Register(node)
	rpc.HandleHTTP()
	http.ListenAndServe(fmt.Sprintf(":%d", listenPort), nil)
}

// FS Notify to watch file changes

// https://github.com/fsnotify/fsnotify

// https://github.com/radovskyb/watcher

// https://medium.com/@skdomino/watch-this-file-watching-in-go-5b5a247cf71f

func main() {
	log.SetFlags(log.Lmicroseconds)
	rand.Seed(time.Now().UnixNano())

	listenPort := 9000
	dataDir := ""

	println(len(os.Args))

	if len(os.Args) == 2 {
		dataDir = os.Args[1]
	} else if len(os.Args) == 3 {
		dataDir = os.Args[1]
		listenPort, _ = strconv.Atoi(os.Args[2])
	} else {
		println("usage : go run filenode.go <data_directory> <listener_port>")
		os.Exit(0)
	}

	println("Starting node listening on port:", listenPort)
	println("data directory : ", dataDir)
	createNode(listenPort, dataDir)
}
