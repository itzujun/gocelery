package errs

import "fmt"

type ErrCouldNotUnmarshaTaskSignature struct {
	msg    []byte
	reason string
}


// Error implements the error interface
func (e ErrCouldNotUnmarshaTaskSignature) Error() string {
	return fmt.Sprintf("Could not unmarshal '%s' into a task signature: %v", e.msg, e.reason)
}

func NewErrCouldNotUnmarshaTaskSignature(msg []byte, err error) ErrCouldNotUnmarshaTaskSignature {
	return ErrCouldNotUnmarshaTaskSignature{msg: msg, reason: err.Error()}
}




