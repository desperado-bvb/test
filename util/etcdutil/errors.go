package etcdutil

import "fmt"

const (
	ErrCodeNotFound int = iota + 1
	ErrCodeKeyExists
)

var errCodeToMessage = map[int] string {
	ErrCodeKeyNotFound:	"key not found",
	ErrCodeKeyExists:	"key exists",
}

func NewKeyNotFoundError(key string) *RegistryError {
	return &StorageError{
		Code:            ErrCodeKeyNotFound,
		Key:             key,
	}
}

func NewKeyExistsError(key string) *RegistryError {
	return &StorageError{
		Code:            ErrCodeKeyExists,
		Key:             key,
	}
}

type Error struct {
	Code			int
	Key			string
	AdditionalErrorMsg	string
}

func (r *Error) Error() string {
	return fmt.Sprintf("RegistryError: %s, Code : %d, Key: %s, AddtionalErrorMsg: %s",
		errorCodeToMessage[r.Code], r.Code, r.Key, r.AdditionalErrorMsg)
}

func IsNotFound(err error) bool {
	return isErrCode(err, ErrCodeKeyNotFound)
}

func IsNotExist(err error) bool {
	return isErrCode(err, ErrCodeKeyExists)
}

func isErrCode(err error, code int) bool {
	if err == nil {
		return false
	}

	if r, ok := err.(*RegistryError); ok {
		return r.Code == code
	}

	return false
}
