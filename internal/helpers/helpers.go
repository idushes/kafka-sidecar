package helpers

func InArrayString(a []string, s string) bool {
	for _, s2 := range a {
		if s == s2 {
			return true
		}
	}

	return false
}