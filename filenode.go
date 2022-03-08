package main

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

const nodeCount = 3

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
	IpAddress        net.IP
	dataDirectory    string
	changeIgnore     map[string]bool

	// stubbing
	shouldStub bool

	//encryption
	gcm       cipher.AEAD
	nonceSize int
}

type EntryLog struct {
	Id        int
	FileName  string
	FilePath  string
	FileData  []byte
	EventType int
	NodeID    int
	IpAddress net.IP

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
		println("Recieved:", latestNodeLog.Index, latestNodeLog.Term, latestNodeLog.FileName, latestNodeLog.EventType, latestNodeLog.FileData)
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
	// n.logs = append(n.logs, *log)

	println("Leader: ", n.logs[len(n.logs)-1].Index, n.logs[len(n.logs)-1].Term, n.logs[len(n.logs)-1].FileName, n.logs[len(n.logs)-1].FileData, n.logs[len(n.logs)-1].EventType)
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

func writeToDataDirectory(node *FileServerNode, fileName string, data []byte) bool {
	filePath := fmt.Sprintf("%s%c%s", node.dataDirectory, os.PathSeparator, fileName)
	ignoreNextChange(node, filePath)

	newEntryLog := EntryLog{
		Id:        (len(node.logs)),
		FileName:  fileName,
		FileData:  []byte(data),
		EventType: Write,
		NodeID:    node.id,
		IpAddress: node.IpAddress,
		Term:      node.term,
		Index:     len(node.logs) + 1,
	}
	node.logs = append(node.logs, newEntryLog)

	writeErr := os.WriteFile(filePath, data, 0644)
	if writeErr != nil {
		println("Unable to write to file", filePath, writeErr)
		return false
	}

	return true
}

func (node *FileServerNode) ReceiveCreateFile(file FileMessage, reply *int) error {
	println("received file - name", file.FileName, "data - ", string(file.Data))
	*reply = 1

	decryptedData, decryptErr := decrypt(node, file.Data)
	if decryptErr != nil {
		*reply = 0
		println("Unable to decrypt data")
	}

	newEntryLog := EntryLog{
		Id:        (len(node.logs)),
		FileName:  file.FileName,
		FileData:  []byte(file.Data),
		EventType: Write,
		NodeID:    node.id,
		IpAddress: node.IpAddress,
		Term:      node.term,
		Index:     len(node.logs) + 1,
	}
	node.logs = append(node.logs, newEntryLog)

	writeSuccess := writeToDataDirectory(node, file.FileName, decryptedData)
	if !writeSuccess {
		*reply = 0
	}

	return nil

}

func sendFileData(node *FileServerNode, fileName string, data []byte) {
	var message = FileMessage{
		FileName: getRelativeFileName(node, fileName),
		Data:     encrypt(node, data),
	}

	client, httpError := rpc.DialHTTP("tcp", "localhost:9002")
	if httpError != nil {
		println("could not communicate with node - ")
		return
	}
	res := 0

	newEntryLog := EntryLog{
		Id:        (len(node.logs)),
		FileName:  fileName,
		FileData:  []byte(message.FileName),
		EventType: Write,
		NodeID:    node.id,
		IpAddress: node.IpAddress,
		Term:      node.term,
		Index:     len(node.logs) + 1,
	}
	node.logs = append(node.logs, newEntryLog)
	println(len(node.logs))

	receiveErr := client.Call("FileServerNode.ReceiveCreateFile", message, &res)
	if receiveErr != nil || res != 1 {
		println("error while sending file")
		return
	}
}

func sendFile(node *FileServerNode, fileName string) {
	// read file contents
	rawFileData, readErr := os.ReadFile(fileName)
	if readErr != nil {
		println("UNABLE TO READ FILE", fileName)
		return
	}

	sendFileData(node, fileName, rawFileData)
}

func sendFileStub(node *FileServerNode, fileName string) {
	stubData := []byte("file stub for fMitosis file larger than 1mb")

	sendFileData(node, fmt.Sprintf("%s.fmit", fileName), stubData)
}

func getRelativeFileName(node *FileServerNode, filename string) string {
	return strings.Replace(filename, node.dataDirectory+"/", "", 1)
}

func (node *FileServerNode) RequestDowloadFullFile(stubName string, reply *int) error {
	println("attempting to download full file", stubName)

	*reply = 1

	fileName := stubName[:len(stubName)-5]

	// download file
	message := FileMessage{}

	client, httpError := rpc.DialHTTP("tcp", "localhost:9001")
	if httpError != nil {
		println("could not communicate with node - ")
		return httpError
	}

	receiveErr := client.Call("FileServerNode.FileDownload", fileName, &message)
	if receiveErr != nil {
		println("error while sending file", receiveErr.Error())
		return receiveErr
	}

	// write the new file
	writeSuccess := writeToDataDirectory(node, fileName, message.Data)
	if !writeSuccess {
		*reply = 0
		return nil
	}

	// remove file stub
	stubPath := fmt.Sprintf("%s%c%s", node.dataDirectory, os.PathSeparator, stubName)
	println("remove stub", stubPath)

	ignoreNextChange(node, stubPath)

	removeStubSuccess := os.Remove(stubPath)
	if removeStubSuccess != nil {
		*reply = 0
		println("unable to remove", stubPath)
		return removeStubSuccess
	}

	return nil
}

func (node *FileServerNode) FileDownload(fileName string, reply *FileMessage) error {
	println("serving", fileName)

	message := FileMessage{
		FileName: fileName,
	}

	filePath := fmt.Sprintf("%s/%s", node.dataDirectory, fileName)

	var readErr error
	message.Data, readErr = os.ReadFile(filePath)
	if readErr != nil {
		println("unable to read file ", filePath)
		return readErr
	}

	*reply = message

	return nil
}

func (node *FileServerNode) ReceiveDeleteFile(fileName string, reply *int) error {
	println("deleting file at", fileName)

	*reply = 1
	baseFileName := strings.Replace(fileName, ".fmit", "", 1)

	filePath := fmt.Sprintf("%s%c%s", node.dataDirectory, os.PathSeparator, baseFileName)
	stubPath := fmt.Sprintf("%s%c%s.fmit", node.dataDirectory, os.PathSeparator, baseFileName)

	toDelete := ""

	// of file exists
	if _, err := os.Stat(filePath); err == nil {
		toDelete = filePath
	} else if _, err := os.Stat(stubPath); err == nil {
		toDelete = stubPath
	}

	if len(toDelete) != 0 {
		ignoreNextChange(node, toDelete)

		deleteErr := os.Remove(toDelete)
		if deleteErr != nil {
			*reply = 0
			println("Unable to delete file", toDelete, deleteErr.Error())
		}
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

	newEntryLog := EntryLog{
		Id:        (len(node.logs)),
		FileName:  fileName,
		FileData:  []byte{},
		EventType: Delete,
		NodeID:    node.id,
		IpAddress: node.IpAddress,
		Term:      node.term,
		Index:     len(node.logs) + 1,
	}
	node.logs = append(node.logs, newEntryLog)

	receiveErr := client.Call("FileServerNode.ReceiveDeleteFile", relativeFileName, &res)
	if receiveErr != nil || res != 1 {
		println("error while deleting file")
		return
	}

}

func (node *FileServerNode) ReceiveCreateDirectory(dir string, reply *int) error {
	println("creating directory at", dir)

	*reply = 1

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

// encryption
func initEncryption(node *FileServerNode) {
	key, readErr := os.ReadFile("encryptionkey.txt")
	if readErr != nil {
		panic("UNABLE TO READ ENCRYPTION KEY")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		fmt.Printf("Error reading key: %s\n", err.Error())
		os.Exit(1)
	}

	fmt.Printf("Key: %s\n", hex.EncodeToString(key))

	node.gcm, err = cipher.NewGCM(block)
	if err != nil {
		fmt.Printf("Error initializing AEAD: %s\n", err.Error())
		os.Exit(1)
	}

	node.nonceSize = node.gcm.NonceSize()
}

func randBytes(length int) []byte {
	b := make([]byte, length)
	rand.Read(b)
	return b
}

func encrypt(node *FileServerNode, plaintext []byte) (ciphertext []byte) {
	nonce := randBytes(node.nonceSize)
	c := node.gcm.Seal(nil, nonce, plaintext, nil)
	return append(nonce, c...)
}

func decrypt(node *FileServerNode, ciphertext []byte) (plaintext []byte, err error) {
	if len(ciphertext) < node.nonceSize {
		return nil, fmt.Errorf("Ciphertext too short.")
	}
	nonce := ciphertext[0:node.nonceSize]
	msg := ciphertext[node.nonceSize:]
	return node.gcm.Open(nil, nonce, msg, nil)
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
				if strings.HasPrefix(filepath.Base(event.Name), ".") {
					// println("ignoring event for file", event.Name)
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

				if event.Op&fsnotify.Rename == fsnotify.Rename {
					deleteFile(node, event.Name)
					continue
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
						// check size of file
						fil, _ := os.Stat(event.Name)
						// if its less than a megabyte, send
						println("filesize", fil.Size())
						if fil.Size() < 1048576 || !node.shouldStub {
							println("sending full file")
							sendFile(node, event.Name)
						} else {
							println("sending stub")
							// otherwise send just the stub
							sendFileStub(node, event.Name)
						}
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
	if listenPort == 9000 {
		node.status = Leader
	}
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

	// set to false to always send full files
	node.shouldStub = true

	node.changeIgnore = make(map[string]bool)

	node.dataDirectory = dataDirectory

	node.IpAddress = GetOutboundIP()

	println(node.IpAddress)
	println(node.IpAddress.To16().String())

	initEncryption(node)

	go WatchTimer(node)
	go sendHeartbeatWhenLeader(node)
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

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}
