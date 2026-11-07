package model

// ServiceRef names one port in a tenant-scoped service catalog.
type ServiceRef struct {
	Service string `json:"service"`
	Port    string `json:"port"`
}

func (r ServiceRef) Valid() bool { return ExactName(r.Service) && ExactName(r.Port) }
