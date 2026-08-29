package main

import (
	"math/rand"
	"strconv"
	"topology"
)

func Execute(id uint32) (*topology.Tuple, bool) {
	t := topology.Tuple{ID: id}
	if id >= 2000 {
		return &t, true
	}

	t.Fields = make(map[string]string)
	i := rand.Int() % 25
	s := strconv.Itoa(i)
	t.Fields["number"] = s

	return &t, false
}
