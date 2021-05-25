package utils

type ErrIndexNotFound struct{}

func (e *ErrIndexNotFound) Error() string {
	return "index not found"
}
