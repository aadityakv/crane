// Package config contains shared runtime configuration and service metadata.
package config

// Transport identifies the network transport used by a service endpoint.
type Transport uint8

const (
	TransportTCP Transport = iota
	TransportUDP
)

// Service identifies a registered runtime service.
type Service uint8

const (
	ServiceSWIMPing Service = iota
	ServiceSWIMACK
	ServiceSWIMSnapshot
	ServiceFileRPC
	ServiceGrepRPC
	ServiceCraneWorker
	ServiceTopologyControl
	ServiceCraneTupleACK
	ServiceRaftRPC
)

// ServiceSpec describes a service's stable name, port offset, and transport.
type ServiceSpec struct {
	Service   Service
	Name      string
	Offset    int
	Transport Transport
}

var serviceSpecs = [9]ServiceSpec{
	{Service: ServiceSWIMPing, Name: "swim-ping", Offset: 0, Transport: TransportUDP},
	{Service: ServiceSWIMACK, Name: "swim-ack", Offset: 1, Transport: TransportUDP},
	{Service: ServiceSWIMSnapshot, Name: "swim-snapshot", Offset: 2, Transport: TransportTCP},
	{Service: ServiceFileRPC, Name: "file-rpc", Offset: 3, Transport: TransportTCP},
	{Service: ServiceGrepRPC, Name: "grep-rpc", Offset: 4, Transport: TransportTCP},
	{Service: ServiceCraneWorker, Name: "crane-worker", Offset: 5, Transport: TransportTCP},
	{Service: ServiceTopologyControl, Name: "topology-control", Offset: 6, Transport: TransportTCP},
	{Service: ServiceCraneTupleACK, Name: "crane-tuple-ack", Offset: 7, Transport: TransportUDP},
	{Service: ServiceRaftRPC, Name: "raft-rpc", Offset: 8, Transport: TransportTCP},
}

// Services returns a copy of the authoritative service registry.
func Services() []ServiceSpec {
	result := make([]ServiceSpec, len(serviceSpecs))
	copy(result, serviceSpecs[:])
	return result
}

// LookupService returns the specification for service, if it is registered.
func LookupService(service Service) (ServiceSpec, bool) {
	if int(service) < 0 || int(service) >= len(serviceSpecs) {
		return ServiceSpec{}, false
	}
	spec := serviceSpecs[service]
	return spec, spec.Service == service
}
