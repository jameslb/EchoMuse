package aecdiag

func validFilterMeta(m Meta) bool {
	if m.FilterGeneration == 0 && m.AEC {
		return false
	}
	if m.FilterDomain != "gained_s16" && m.FilterDomain != "unity_f32_s24" {
		return false
	}
	switch m.FilterLoadStatus {
	case "inactive", "not_attempted", "missing", "unreadable":
		return m.FilterLoadSHA256 == "" && m.FilterLoadBytes == 0
	case "loaded", "refused":
		if m.FilterLoadBytes < 1 {
			return false
		}
		if m.FilterLoadBytes > 131072 {
			return m.FilterLoadStatus == "refused" && m.FilterLoadSHA256 == ""
		}
		if len(m.FilterLoadSHA256) != 64 {
			return false
		}
		for _, c := range m.FilterLoadSHA256 {
			if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
				return false
			}
		}
		return true
	}
	return false
}
