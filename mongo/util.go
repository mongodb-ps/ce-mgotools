package mongo

import "strings"

func GetCollectionFromNamespace(ns string) string {
	parts := strings.Split(ns, ".")
	if len(parts) <= 1 {
		return ""
	} else {
		return strings.Join(parts[1:], ".")
	}
}
