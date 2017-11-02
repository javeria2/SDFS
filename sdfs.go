package main

import (
  "SDFS/protocol-buffer"
  "fmt"
  "io"
  "io/ioutil"
  "os"
  "net"
  "github.com/golang/protobuf/proto"
  "time"
)

/****************************************/
/****************  SDFS  ****************/
/****************************************/
const (
	primaryMaster   = 1
	secondaryMaster = 2
	thirdMaster     = 3
	sdfsPort        = ":4040"
)

var (
  fileMap    = make(map[string][]uint32)
	sdfsPacket = &heartbeat.SdfsPacket{Source: uint32(vmID)}
)

/**
 File op: PUT, put a local file with filename @localFileName into sdfs with file name @sdfsFileName
*/
func putFile(localFileName string, sdfsFileName string) {
	if vmID == primaryMaster {
		updateFileMap(sdfsFileName, uint32(vmID))
	} else {
		// not main master, send msg to master and add files into filemap
		sendSDFSMessage(primaryMaster, "add", sdfsFileName, vmID, nil)
	}
	// makeLocalReplicate(sdfsFileName, localFileName)
	// replicate(sdfsFileName, vmID)
}

/**
  File op: STORE, Prints the files on the current node.
*/
func store() {

}

/**
 File op: GET
*/
func getFile(localFileName string, sdfsFileName string) {

}

/**
 File op: DELETE
*/
func deleteFile(sdfsFileName string) {

}

/**
 File op: LSFILE
*/
func lsFile(sdfsFileName string) {

}

/**
	Utility method to update current nodes filemap.
*/
func updateFileMap(sdfsFileName string, vmID uint32) {
	var firstPeer = vmID + 1
	var secondPeer = vmID + 2
	if secondPeer > 10 {
		secondPeer = 1
	}
	if firstPeer > 10 {
		firstPeer = 1
		secondPeer = 2
	}
	//update the current nodes filemap
	fileMap[sdfsFileName] = append(fileMap[sdfsFileName], vmID)
	fileMap[sdfsFileName] = append(fileMap[sdfsFileName], firstPeer)
	fileMap[sdfsFileName] = append(fileMap[sdfsFileName], secondPeer)
}

/**
	Make local replica of file with name @sdfsFileName,@localFileName should already exist.
	https://stackoverflow.com/questions/21060945/simple-way-to-copy-a-file-in-golang
*/
func makeLocalReplicate(sdfsFileName string, localFileName string) {
	in, err := os.Open(localFileName)
  if err != nil {
		fmt.Println("Error: ", err)
		myLog.Fatal(err)
    return
  }
  defer in.Close()
  out, err := os.Create(sdfsFileName)
  if err != nil {
		fmt.Println("Error: ", err)
		myLog.Fatal(err)
    return
  }
	defer out.Close()
  if _, err = io.Copy(out, in); err != nil {
		fmt.Println("Error: ", err)
		myLog.Fatal(err)
    return
  }
	//flush the file to stable storage
  out.Sync()
}

/**
	Replicate the file with name sdfsFileName on nodeID + 1, nodeID + 2.
*/
func replicate(sdfsFileName string, nodeID int) {
	var firstPeer = nodeID + 1
	var secondPeer = nodeID + 2
	if secondPeer > 10 {
		secondPeer = 1
	}
	if firstPeer > 10 {
		firstPeer = 1
		secondPeer = 2
	}
	fi, err := ioutil.ReadFile(sdfsFileName)
	if err != nil {
		fmt.Printf("error %s\n", err)
		return
	}
	sendSDFSMessage(firstPeer, "file", sdfsFileName, vmID, fi)
	sendSDFSMessage(secondPeer, "file", sdfsFileName, vmID, fi)
}

/**
	Send an sdfs message packet to node with id @nodeID
*/
func sendSDFSMessage(nodeID int, message string, sdfsFileName string, vmID int, file []byte) {
	if iHaveLeft {
		// Do nothing if the node has left
		time.Sleep(time.Nanosecond)
		return
	}

	// construct our msg
	sdfsPacket.Msg = message
	sdfsPacket.SdfsFileName = sdfsFileName
	if file != nil {
		sdfsPacket.File = file
	}

	//Marshal the msg
	m, err := proto.Marshal(sdfsPacket)
	if err != nil {
		fmt.Printf("error %s\n", err)
		return
	}

	conn, err := net.Dial("udp", fmt.Sprintf(nodeName, nodeID, sdfsPort))
	if err != nil {
		fmt.Printf("error %s\n", err)
		return
	}
	//defer close and write message to tcp connection
	defer conn.Close()
	conn.Write(m)
}

/**
	Receive an sdfs message packet.
*/
func receiveSDFSMessage() {
	//set up tcp listener
	conn, err := net.ListenPacket("udp", sdfsPort)
	if err != nil {
		fmt.Printf("error %s\n", err)
		return
	}
	defer conn.Close()

	buf := make([]byte, 1200)
	for {
		if iHaveLeft {
			// do not update anything if the node has left
			time.Sleep(time.Nanosecond)
			continue
		}

		// continue listenning
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			fmt.Println("error: ", err)
			return
		}
		sdfsMsg := &heartbeat.SdfsPacket{}
		if err := proto.Unmarshal(buf[0:n], sdfsMsg); err != nil {
			fmt.Printf("Failed. Error: %s\n", err)
			return
		}
		// fmt.Println("n: ", n)
		// fmt.Println(proto.MarshalTextString(sdfsMsg))
		myLog.Printf("Message sent from node %d (IP: %s).\n", sdfsMsg.GetSource(), addr.String())
		switch sdfsMsg.GetMsg() {
			case "add":
				updateFileMap(sdfsMsg.GetSdfsFileName(), sdfsMsg.GetSource())
			case "file":
				saveFile(sdfsMsg.GetSdfsFileName(), sdfsMsg.GetFile())
		}
	}
}

/**
	Save file @file locally with name @sdfsFileName
*/
func saveFile(sdfsFileName string, file []byte) {

}

/**
	Utility function to print file map of node with node id @VMid
*/
func printFileMap() {
	fmt.Println("name id")
	for k, v := range fileMap {
		for _, idx := range v {
			fmt.Printf("%s %d\n", k, idx)
		}
	}
}
