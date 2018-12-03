package main

import "errors"

var (
	errArgsIsNil      = errors.New("args is nil")
	errArgsNotAvail   = errors.New("args set not available")
	errFileNotExist   = errors.New("file not exist")
	errMemberNotExist = errors.New("member not exist")
)
