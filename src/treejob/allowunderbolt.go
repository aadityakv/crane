package main

import (
	"strconv"
	"topology"
)

func Execute(tup *topology.Tuple) (*topology.Tuple, bool) {
	v, ok := tup.Fields["number"]

	if !ok {
		return tup, false
	}

	i, err := strconv.Atoi(v)

	if err != nil {
		return tup, false
	}

	if i < 10 {
		tup.Fields["number"] = "KAZAAAM"
	} else {
		tup.Fields["number"] = "SHAZAAAM"
	}

	return tup, true
}
