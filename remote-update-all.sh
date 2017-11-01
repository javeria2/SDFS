#!/usr/bin/env bash

for (( i = 1; i < 11; i++ )); do
    var=$(printf "weiren2@fa17-cs425-g40-%02d.cs.illinois.edu" $i)
    # ssh $var 'export PATH=/usr/local/go/bin:$PATH; cd ~/go/src/cs425_mp2 ; git fetch; git checkout false-positive; git pull; go version; go get; go build' &
    ssh $var 'cd SDFS; git pull; go build' &
done
wait
