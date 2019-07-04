package common

import "github.com/itzujun/gocelery/config"

type Backend struct {
	cnf *config.Config
}

func NewBackend(cnf *config.Config) Backend {
	return Backend{cnf: cnf}
}


func (b *Backend) GetConfig() *config.Config {
	return b.cnf
}


func (b *Backend) IsAMQP() bool {
	return false
}
