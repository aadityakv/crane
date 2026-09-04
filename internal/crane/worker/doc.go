// Package worker implements the Crane worker: the process-local owner that
// executes assigned tasks and keeps their results durable. Service composes
// the pieces for one node: Engine drives operators over durable deliveries
// and sends downstream tuples, TupleService receives them, ControlOwner
// serializes coordinator commands over authenticated control sessions,
// TransferOwner and RepairDriver move result records and sealed artifacts
// between replicas, and Repository is the store-backed authority they all
// consult. The package guards the invariant that every state transition is
// validated against the current coordinator fence and installed assignment
// revision before it is persisted, so a superseded coordinator or a stale
// assignment can never mutate worker state.
package worker
