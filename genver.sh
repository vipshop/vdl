#!/bin/bash

gitlog=`git log --date=iso --pretty=format:"%cd @%h" -1`
if [ $? -ne 0 ]; then
    version="not a git repo"
fi

compile=`date +"%F %T %z"`" by "`go version`

cat << EOF | gofmt > version/gitlog.go
package version

const (
    GitLog = "${gitlog}"
    Compile = "${compile}"
)
EOF