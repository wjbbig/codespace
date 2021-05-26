package utils

import "github.com/pkg/errors"

var (
	ErrIndexNotFound       = errors.New("index not found")
	ErrUnexpectedEndOfFile = errors.New("unexpected end of file")
)
