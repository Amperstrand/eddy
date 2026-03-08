package shortid

// Short truncates long hex strings (pubkeys, cycle IDs) for logging.
func Short(s string) string {
	if len(s) <= 16 {
		return s
	}
	return s[:16]
}
