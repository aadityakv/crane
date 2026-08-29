// Package node provides lifecycle supervision for node services.
package node

import "context"

// Service is a long-running node component supervised by Supervisor. Ready
// must be closed after the service has bound all required listeners. Run must
// return when its context is canceled.
type Service interface {
	Name() string
	Ready() <-chan struct{}
	Run(context.Context) error
}
