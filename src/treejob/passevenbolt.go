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

	if i%2 == 1 {
		return tup, true
	} else {
		return tup, false
	}
}
