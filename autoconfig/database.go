package autoconfig

import (
	"go-uk-maps-import/database/engine"
)

var (
	defaultHost    = "127.0.0.1"
	defaultUser    = "osdata"
	defaultPass    = "osdata"
	defaultSchema  = "osdata"
	defaultTimeout = 10
)

func getHost(userConfig engine.DBConfig) string {
	if userConfig.Host == nil {
		return defaultHost
	}
	return *userConfig.Host
}

func getPort(userConfig engine.DBConfig, defaultPort string) string {
	if userConfig.Port == nil {
		return defaultPort
	}
	return *userConfig.Port
}

func getUser(userConfig engine.DBConfig) string {
	if userConfig.User == nil {
		return defaultUser
	}
	return *userConfig.User
}

func getPass(userConfig engine.DBConfig) string {
	if userConfig.Pass == nil {
		return defaultPass
	}
	return *userConfig.Pass
}

func getSchema(userConfig engine.DBConfig) string {
	if userConfig.Schema == nil {
		return defaultSchema
	}
	return *userConfig.Schema
}

func getTimeout(userConfig engine.DBConfig) int {
	if userConfig.Timeout == nil {
		return defaultTimeout
	}
	return *userConfig.Timeout
}
