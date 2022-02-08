package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
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

type FileServerNode struct {
	id               int
	election_timeout int
	status           int
	term             int
	leader_id        int
	timeout_ticker   time.Ticker
	received_hb      bool
	logs             []EntryLog
}

type EntryLog struct {
	id    int
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

func watchForChanges(directory string) {
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
				log.Println("event:", event)
				if event.Op&fsnotify.Write == fsnotify.Write {
					log.Println("modified file:", event.Name)
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

	err = watcher.Add(directory)
	if err != nil {
		log.Fatal(err)
	}
	<-done
}

func createNode(listenPort int) {
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

	// go WatchTimer(node)
	// go sendHeartbeatWhenLeader(node)
	go watchForChanges("/datadirectory")

	rpc.Register(node)
	rpc.HandleHTTP()
	http.ListenAndServe(fmt.Sprintf(":%d", listenPort), nil)
}

// FS Notify to watch file changes

// https://github.com/fsnotify/fsnotify

// https://medium.com/@skdomino/watch-this-file-watching-in-go-5b5a247cf71f

func main() {
	rand.Seed(time.Now().UnixNano())

	listenPort := 9000

	if len(os.Args) == 2 {
		listenPort, _ = strconv.Atoi(os.Args[1])
	}

	println("Starting node listening on port:", listenPort)
	createNode(listenPort)
}
