package main

import "errors"

var (
	errArgsNotAvail = errors.New("args not available")
	errDirNotAvail  = errors.New("dir not available")
	errSnapMeta     = errors.New("snapshot metadata is incomplete")
	errNotMatch     = errors.New("file name not match")
)
