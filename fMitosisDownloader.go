package main

import (
	// "net/rpc"
	"net/rpc"
	"os"
	"path/filepath"
)

// "log"
// "math/rand"
// "os"
// "strconv"
// "time"

func main() {

	println("downloading")
	println(len(os.Args))

	if len(os.Args) != 2 {
		println("usage : go run filedownloader.go <filestub.fmit>")
		os.Exit(0)
	}

	fileName := filepath.Base(os.Args[1])

	println("requesting download of", fileName)

	// THIS RPC ENDPOINT SHOULD BE THE 'LOCAL' ONE
	client, httpError := rpc.DialHTTP("tcp", "localhost:9002")
	if httpError != nil {
		println("could not communicate with node - ")
		return
	}
	res := 0

	receiveErr := client.Call("FileServerNode.RequestDowloadFullFile", fileName, &res)
	if receiveErr != nil || res != 1 {
		println("error while requesting the file download")
		return
	}

}
