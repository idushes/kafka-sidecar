package helpers

import "strings"

func InArrayString(a []string, s string) bool {
	for _, s2 := range a {
		if s == s2 {
			return true
		}
	}

	return false
}

func RemoveEmptyStrings(s []string) []string {
	cleaned := make([]string, 0, len(s))
	for _, topic := range s {
		t := strings.TrimSpace(topic)
		if len(t) > 0 {
			cleaned = append(cleaned, t)
		}
	}

	return cleaned
}
