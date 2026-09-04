// Package config contains shared runtime configuration and service metadata.
package config

// Transport identifies the network transport used by a service endpoint.
type Transport uint8

const (
	// TransportUDP identifies a datagram endpoint.
	TransportUDP Transport = iota
	// TransportTCP identifies a stream endpoint.
	TransportTCP
)

// Service identifies a registered runtime service.
type Service uint8

const (
	// ServiceSWIMPing carries direct probes, indirect requests, gossip, and digests.
	ServiceSWIMPing Service = iota
	// ServiceSWIMACK carries direct and relayed probe acknowledgments.
	ServiceSWIMACK
	// ServiceSWIMSnapshot carries join and membership snapshot exchanges.
	ServiceSWIMSnapshot
	// ServiceCraneWorker carries coordinator-to-worker control sessions and worker-to-worker result transfer.
	ServiceCraneWorker
	// ServiceTopologyControl carries the public client API (submit, cancel, status, results).
	ServiceTopologyControl
	// ServiceCraneTupleACK carries authenticated tuple deliveries and their acknowledgments.
	ServiceCraneTupleACK
	// ServiceRaftRPC identifies the active fixed-voter Raft RPC endpoint.
	ServiceRaftRPC
)

// ServiceSpec describes a service's stable name, port offset, and transport.
type ServiceSpec struct {
	// Service is the stable typed registry key.
	Service Service
	// Name is the stable human-readable service label.
	Name string
	// Offset is added to a validated base port with checked arithmetic.
	Offset uint16
	// Transport selects the listener/socket kind for this service.
	Transport Transport
}

var serviceSpecs = [7]ServiceSpec{
	{Service: ServiceSWIMPing, Name: "swim-ping", Offset: 0, Transport: TransportUDP},
	{Service: ServiceSWIMACK, Name: "swim-ack", Offset: 1, Transport: TransportUDP},
	{Service: ServiceSWIMSnapshot, Name: "swim-snapshot", Offset: 2, Transport: TransportTCP},
	{Service: ServiceCraneWorker, Name: "crane-worker", Offset: 3, Transport: TransportTCP},
	{Service: ServiceTopologyControl, Name: "topology-control", Offset: 4, Transport: TransportTCP},
	{Service: ServiceCraneTupleACK, Name: "crane-tuple-ack", Offset: 5, Transport: TransportUDP},
	{Service: ServiceRaftRPC, Name: "raft-rpc", Offset: 6, Transport: TransportTCP},
}

// Services returns a copy of the authoritative service registry.
func Services() []ServiceSpec {
	result := make([]ServiceSpec, len(serviceSpecs))
	copy(result, serviceSpecs[:])
	return result
}

// LookupService returns the specification for service, if it is registered.
func LookupService(service Service) (ServiceSpec, bool) {
	if int(service) >= len(serviceSpecs) {
		return ServiceSpec{}, false
	}
	spec := serviceSpecs[service]
	return spec, spec.Service == service
}
