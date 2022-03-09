package main

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"io/fs"
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

var nodeAddresses = []string{"localhost:9000", "localhost:9001", "localhost:9002"}

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
	hostname         string
	election_timeout int
	status           int
	term             int
	leader_id        string
	timeout_ticker   time.Ticker
	received_hb      bool
	logs             []EntryLog
	IpAddress        net.IP
	dataDirectory    string
	changeIgnore     map[string]bool
	init_hb          bool

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
	NodeID    string
	IsDir     bool
	IpAddress net.IP

	Term  int
	Index int
}

type Heartbeat struct {
	Term      int
	Leader_id string
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

	for _, node := range nodeAddresses {
		// Rather than sending RPC call to yourself, just vote for yourself.
		if node == n.hostname {
			voteCount++
			continue
		}

		client, httpError := rpc.DialHTTP("tcp", node)
		if httpError != nil {
			println(node, " - offline")
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

		println(node, "voted", res)

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
		// println("Heartbeat receieved")
		// println("received heartbeat from leader", heartbeat.Leader_id, heartbeat.Term)
		if !n.init_hb {
			InitializeNewNode(n)
			n.init_hb = true
		}
	}

	*reply = Ack
	return nil
}

func WriteToLeaderLog(event fsnotify.Event, node *FileServerNode) {
	if node.status != Leader {
		return
	}
	if event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Write == fsnotify.Write {
		rawFileData, readErr := os.ReadFile(event.Name)
		if readErr != nil {
			println("UNABLE TO READ FILE", event.Name)
		}
		fileStat, err := os.Stat(event.Name)
		if err != nil {
			log.Println(err)
		}

		fmt.Println("Is Directory: ", fileStat.IsDir())

		println("File found:", event.Name)
		parsedFileName := strings.Replace(event.Name, node.dataDirectory+"/", "", 1)
		newEntryLog := EntryLog{
			Id:        (len(node.logs)),
			FileName:  parsedFileName,
			FileData:  rawFileData,
			EventType: Write,
			NodeID:    node.hostname,
			IpAddress: node.IpAddress,
			Term:      node.term,
			IsDir:     fileStat.IsDir(),
			Index:     len(node.logs) + 1,
		}
		node.logs = append(node.logs, newEntryLog)
	} else if event.Op&fsnotify.Remove == fsnotify.Remove {
		parsedFileName := strings.Replace(event.Name, node.dataDirectory+"/", "", 1)
		newEntryLog := EntryLog{
			Id:        (len(node.logs)),
			FileName:  parsedFileName,
			FileData:  []byte{},
			EventType: Delete,
			NodeID:    node.hostname,
			IpAddress: node.IpAddress,
			Term:      node.term,
			IsDir:     false,
			Index:     len(node.logs) + 1,
		}
		node.logs = append(node.logs, newEntryLog)
	}
	println(len(node.logs))
}

func (n *FileServerNode) RequestLogs(requestingNode *FileServerNode, reply *[]EntryLog) error {
	if n.status == Leader {
		println(len(n.logs))
		*reply = n.logs
	}
	return nil
}

func InitializeNewNode(node *FileServerNode) error {
	if node.leader_id == "" {
		// InitializeNewNode(node)
		return nil
	}
	logs := []EntryLog{}
	client, httpError := rpc.DialHTTP("tcp", node.leader_id)
	if httpError != nil {
		println("could not communicate with node - ", node.leader_id)
		return httpError
	}

	receiveErr := client.Call("FileServerNode.RequestLogs", node, &logs)
	if receiveErr != nil {
		println("error while requesting logs", receiveErr.Error())
		return receiveErr
	}
	println(len(logs))
	println(logs)

	for i := 0; i < len(logs); i++ {
		PerformLogFileChanges(node, logs[i])
	}
	return nil
}

func PerformLogFileChanges(n *FileServerNode, log EntryLog) {
	if log.EventType != Delete {
		if log.IsDir {
			dirPath := fmt.Sprintf("%s%c%s", n.dataDirectory, os.PathSeparator, log.FileName)
			ignoreNextChange(n, dirPath)
			mkdirErr := os.Mkdir(dirPath, 0755)
			if mkdirErr != nil {
				println("Unable to create directory", dirPath, mkdirErr.Error())
			}
		} else {
			writeToDataDirectory(n, log.FileName, log.FileData)
		}
	} else {
		ignoreNextChange(n, log.FileName)

		deleteErr := os.Remove(log.FileName)
		if deleteErr != nil {
			println("Unable to delete file", log.FileName, deleteErr.Error())
		}
	}
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

func sendHeartbeat(n *FileServerNode, sendToNode string) {
	client, httpError := rpc.DialHTTP("tcp", sendToNode)
	if httpError != nil {

		return
	}
	res := 0

	hb := new(Heartbeat)
	hb.Term = n.term
	hb.Leader_id = n.hostname

	clientErr := client.Call("FileServerNode.LeaderHeartbeat", hb, &res)
	if clientErr != nil {
		fmt.Println("Error: FileServerNode.LeaderHeartbeat", clientErr)
		return
	}

}

func sendHeartbeatWhenLeader(node *FileServerNode) {
	for range time.Tick(50 * time.Millisecond) {
		if node.status == Leader {
			// print("sending heartbeats (term ", node.term, ") : ")
			for _, i := range nodeAddresses {
				if i != node.hostname {
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

	writeSuccess := writeToDataDirectory(node, file.FileName, decryptedData)
	if !writeSuccess {
		*reply = 0
	}

	return nil

}

func sendFileData(node *FileServerNode, fileName string, data []byte, nodeAddress string) {
	var message = FileMessage{
		FileName: getRelativeFileName(node, fileName),
		Data:     encrypt(node, data),
	}

	client, httpError := rpc.DialHTTP("tcp", nodeAddress)
	if httpError != nil {
		println("could not communicate with node - ", nodeAddress)
		return
	}
	res := 0

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

	stubData := []byte("file stub for fMitosis file larger than 1mb")

	fil, _ := os.Stat(fileName)
	// if its less than a megabyte, send
	println("filesize", fil.Size())

	for _, dest := range nodeAddresses {
		if dest == node.hostname {
			continue
		}

		if dest == node.leader_id || fil.Size() < 1048576 || !node.shouldStub {
			go sendFileData(node, fileName, rawFileData, dest)
		} else {
			go sendFileData(node, fmt.Sprintf("%s.fmit", fileName), stubData, dest)
		}
	}
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

	client, httpError := rpc.DialHTTP("tcp", node.leader_id)
	if httpError != nil {
		println("could not communicate with node - ", node.leader_id)
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

func deleteFileAll(node *FileServerNode, fileName string) {
	for _, dest := range nodeAddresses {
		if dest != node.hostname {
			go deleteFile(node, fileName, dest)
		}
	}
}

func deleteFile(node *FileServerNode, fileName string, destAddress string) {

	relativeFileName := getRelativeFileName(node, fileName)

	client, httpError := rpc.DialHTTP("tcp", destAddress)
	if httpError != nil {
		println("could not communicate with node - ", destAddress)
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

func createDirectoryAll(node *FileServerNode, dir string) {
	for _, dest := range nodeAddresses {
		if dest != node.hostname {
			go createDirectory(node, dir, dest)
		}
	}
}

func createDirectory(node *FileServerNode, dir string, destintation string) {
	relativeDirName := getRelativeFileName(node, dir)

	client, httpError := rpc.DialHTTP("tcp", destintation)
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
	println("ignoring next change", filePath)
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

func watchPath(node *FileServerNode, path string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}

	println("New watcher watching", path)
	watcher.Add(path)

	defer watcher.Close()
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
				WriteToLeaderLog(event, node)
				if ignoreExists(node, event.Name) {
					println("found ignore for ", event.Name)
					continue
				}
				println("did not find ignore for", event.Name)

			}

			if event.Op&fsnotify.Rename == fsnotify.Rename {
				deleteFileAll(node, event.Name)
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
				deleteFileAll(node, event.Name)
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
					createDirectoryAll(node, event.Name)
					go watchPath(node, event.Name)
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

}

func watchForChanges(node *FileServerNode) {
	filepath.WalkDir(node.dataDirectory, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			go watchPath(node, path)
		}
		return nil
	})
}

func createNode(listenPort int, dataDirectory string, hostname string) {
	node := new(FileServerNode)
	node.status = Follower
	node.hostname = hostname
	// set the leader as uninitialized
	node.leader_id = ""
	// set the election timeout
	node.election_timeout = rand.Intn(3000-1500) + 1500
	// start the timer
	node.timeout_ticker = *time.NewTicker(time.Second)
	// initialize empty logs
	node.logs = []EntryLog{
		{
			Term:  -1,
			Index: 0,
		},
	}

	// set to false to always send full files
	node.shouldStub = true

	node.changeIgnore = make(map[string]bool)

	node.dataDirectory = dataDirectory

	node.IpAddress = GetOutboundIP()

	node.init_hb = false

	initEncryption(node)

	go WatchTimer(node)
	go sendHeartbeatWhenLeader(node)
	watchForChanges(node)
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
	hostname := ""

	if len(os.Args) == 4 {
		dataDir = os.Args[1]
		listenPort, _ = strconv.Atoi(os.Args[2])
		hostname = os.Args[3]
	} else {
		println("usage : go run filenode.go <data_directory> <listener_port> <hostname>")
		os.Exit(0)
	}

	println("Starting node listening on port:", listenPort)
	println("data directory : ", dataDir)
	createNode(listenPort, dataDir, hostname)
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
