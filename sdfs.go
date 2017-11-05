package main

import (
  "SDFS/protocol-buffer"
  "fmt"
  "io"
  "io/ioutil"
  "os"
  "net"
  "time"
  "github.com/golang/protobuf/proto"
)
//TODO: handle ctrl+C and send filemap back, OR line 353, update filemap accordingly (remove stuff).
//whenever a node joins, look into files folder and delete everything.
//update replicas by looking into the membership list
/****************************************/
/****************  SDFS  ****************/
/****************************************/
const (
	sdfsPort = ":4040"
)

var (
  primaryMaster   = 1
  secondaryMaster = 2
  thirdMaster     = 3
  fileMap         = make(map[string]*heartbeat.MapValues)
  currStored      []string
	sdfsPacket      = &heartbeat.SdfsPacket{Source: uint32(vmID)}
)

/**
 File op: PUT, put a local file with filename @localFileName into sdfs with file name @sdfsFileName
*/
func putFile(localFileName string, sdfsFileName string) {
	if vmID == primaryMaster {
		updateFileMap(sdfsFileName, uint32(vmID))
	} else {
		// not main master, send msg to master and add files into filemap
		sendSDFSMessage(primaryMaster, "add", sdfsFileName, nil)
	}
	// makeLocalReplicate(sdfsFileName, localFileName)
	// replicate(sdfsFileName, vmID)
}

/**
  File op: STORE, Prints the files on the current node.
*/
func store() {
  fmt.Printf("Files currently being stored on node %d are: \n", vmID)
  for _, val := range currStored {
    fmt.Println(val)
  }
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
  //simply echo back the request to the primary node
  // sendSDFSMessage(primaryMaster, "ls", sdfsFileName, nil)
  fmt.Printf("%s is present on VMs with VM ids: ", sdfsFileName)
  for k, v := range fileMap {
    if k == sdfsFileName {
      for _, val := range v.GetValues() {
        fmt.Printf("%d, ", val)
      }
      fmt.Println()
      break
    }
  }
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
  var node_ids heartbeat.MapValues /* used for filemap value */
  var vals []uint32
  vals = append(vals, vmID)
  vals = append(vals, firstPeer)
  vals = append(vals, secondPeer)
  node_ids.Values = vals
	fileMap[sdfsFileName] = &node_ids
}

/**
	Make local replica of file with name @sdfsFileName,@localFileName should already exist.
	https://stackoverflow.com/questions/21060945/simple-way-to-copy-a-file-in-golang
*/
func makeLocalReplicate(sdfsFileName string, localFileName string) {
	in, err := os.Open("/files/" + localFileName)
  if err != nil {
		fmt.Println("Error: ", err)
		myLog.Fatal(err)
    return
  }
  defer in.Close()
  out, err := os.Create("/files/" + sdfsFileName)
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
  currStored = append(currStored, sdfsFileName)
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
	sendSDFSMessage(firstPeer, "file", sdfsFileName, fi)
	sendSDFSMessage(secondPeer, "file", sdfsFileName, fi)
}

/**
	Send an sdfs message packet to node with id @nodeID
*/
func sendSDFSMessage(nodeID int, message string, sdfsFileName string, file []byte) {
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

  // make buffer large enough, it can contain files. Here we allow upto 300MB files.
	buf := make([]byte, 3e8)
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
    //handle the requests
    handleRequest(sdfsMsg)
	}
}

/**
  Handle the incoming requests.
*/
func handleRequest(sdfsMsg *heartbeat.SdfsPacket) {
  switch sdfsMsg.GetMsg() {
    case "add":
      updateFileMap(sdfsMsg.GetSdfsFileName(), sdfsMsg.GetSource())
    case "file":
      saveFile(sdfsMsg.GetSdfsFileName(), sdfsMsg.GetFile())
    // case "ls":
    //   loc := fileMap[sdfsMsg.GetSdfsFileName()].GetValues()
    //   locations := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(loc)), ","), "[]")
    //   //send the file locations back to source from primary master, rest is handled in the default case
    //   sendSDFSMessage(int(sdfsMsg.GetSource()), locations, sdfsMsg.GetSdfsFileName(), nil)
    // default: //default case is only when we get the file locations back (ls), simply print them out
    //   fmt.Printf("%s is present on VMs with VM ids: %s\n", sdfsMsg.GetSdfsFileName(),
    //   sdfsMsg.GetMsg())
  }
}

/**
	Save file @file locally with name @sdfsFileName
*/
func saveFile(sdfsFileName string, file []byte) {
  // set permissions, allow r/w/e by everyone in this case
  permission := 0777
  //TODO: might have to decode file base64.StdEncoding.DecodeString
  err := ioutil.WriteFile("/files/" + sdfsFileName, file, os.FileMode(permission))
  if err != nil {
    fmt.Println("Error saving file locally (replication): ", err)
    return
  }
  currStored = append(currStored, sdfsFileName)
}

/**
	Utility function to print file map of node with node id @VMid
*/
func printFileMap() {
	fmt.Println("name id")
	for k, v := range fileMap {
		for _, val := range v.GetValues() {
			fmt.Printf("%s %d\n", k, val)
		}
	}
}

/**
  Tell if current node is master or not, if not, tell the correct master.
*/
func isMaster() {
  fmt.Printf("Primary master is: %d, Secondary master is: %d, Third master is: %d\n",
    primaryMaster, secondaryMaster, thirdMaster)
  if vmID == primaryMaster {
    fmt.Println("Current node is primary master node.")
  } else {
    fmt.Println("Current node is not the primary master.")
  }
}

/**
  Re-election of the master node on failing
*/
func masterElection(){
    tempMaster := 1
    if membershipList[primaryMaster-1].GetStatus() != alive {
        tempMaster = secondaryMaster
        if membershipList[secondaryMaster-1].GetStatus() != alive {
            tempMaster = thirdMaster
        }
        primaryMaster = tempMaster
    }
}
