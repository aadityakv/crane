//go:build !craneintegration

package integrationhook

// Enabled reports whether this binary was built with the craneintegration
// tag. The ordinary build is never enabled.
const Enabled = false

// LoadFromInheritedFD returns the no-op Hook. The ordinary build inspects no
// environment variable, command-line switch, or file descriptor: there is no
// activation path to take.
func LoadFromInheritedFD() Hook { return Noop{} }
