package main

import "errors"

var (
	errArgsNotAvail = errors.New("args not available")
	errSnapMetaNil  = errors.New("snap meta is nil")
	errMd5NotMatch  = errors.New("md5 not match")
	errFileNotExist = errors.New("file not exist")
	errHttp         = errors.New("http resp error")
)
