**Note**: We are using a modified version of the best solution for MP2 because of the lesser number of go routines it had.

**Dependencies**: Our implementation uses Google Protocol Buffers, to run our code, please install the protobuf compiler through [this link](https://github.com/google/protobuf).

However, we've already set this up on our VMs and the code can simply run as under -

Clone the repo and `cd` into the folder. Then run the following (You can skip this step on the VMs) -
```
  go get
```
This should fetch all the dependencies correctly. Then run the following -
```
  go build failure_detector.go sdfs.go
```
This should compile our code correctly. Then run the following -
```
  go run failure_detector.go sdfs.go
```

You should now be able to see our CLI. Please note that you'll have to join each node by typing `join`. This essentially adds it to the SDFS. All the other commands are showed via the CLI and are self explanatory.

All the files included in the SDFS will be stored in the `files/` folder within the same directory. You do not need to create this manually, it'll be created automatically when a node join the SDFS.

However, all the local files will go in the root directory itself. (the folder of the cloned repo)

Thank you.
