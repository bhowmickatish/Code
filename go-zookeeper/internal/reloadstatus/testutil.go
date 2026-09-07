package reloadstatus

// ResetForTest clears recorded reload status between tests.
func ResetForTest() {
	mu.Lock()
	defer mu.Unlock()

	hasRules = false
	rules = ConcernStatus{}

	hasZK = false
	zk = ConcernStatus{}
}
