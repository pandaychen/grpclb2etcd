package utils

import (
	"fmt"
)

func GetGrpcName(scheme string) string {
	return fmt.Sprintf("%s:///", scheme)
}
