package main

var (
  fileMap  map[string][]uint32 // comes from sdfs.go
  isMaster = false
)

/**
  File op: STORE, Prints the files on the current node.
*/
func store() {
  
}

/**
 File op: PUT
*/
func putFile(localFileName string, sdfsFileName string) {

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
