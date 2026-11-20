// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright 2026 The DFaaS Authors. All rights reserved.
// This file is licensed under the AGPL v3.0 or later license. See LICENSE and
// AUTHORS file for more information.

package loadbalancer

import (
	"fmt"
	"math"
	"strconv"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/unimib-datAI/dfaas/dfaasagent/agent/faas"
	"github.com/unimib-datAI/dfaas/dfaasagent/agent/hacfgupd"
	"github.com/unimib-datAI/dfaas/dfaasagent/agent/logging"
)

type CostBasedStrategy struct {
	hacfgupdater *hacfgupd.Updater
	faas         faas.Provider
}

func (strategy *CostBasedStrategy) RunStrategy() error {
	logger := logging.Logger()
	logger.Infof("Starting CostBased strategy")

	ticker := time.NewTicker(_config.RecalcPeriod)
	defer ticker.Stop()

	for range ticker.C {
		start := time.Now().UTC()

		weights, err := strategy.calculateWeights()
		if err != nil {
			return fmt.Errorf("calculating new weights: %w", err)
		}

		if err = strategy.setProxyWeights(weights); err != nil {
			return fmt.Errorf("setting new weights: %w", err)
		}

		duration := time.Since(start)

		logger.Infow("Iteration completed", "duration", duration.String())
	}

	return nil
}

func (strategy *CostBasedStrategy) OnReceived(msg *pubsub.Message) error {
	return nil
}

func calcolaPesi(
	myNodeID string,
	CL float64,
	CR map[string]float64,
) (map[string]uint, error) {

	if CL <= 0 {
		return nil, fmt.Errorf(
			"local cost must be greater than 0, got %f",
			CL,
		)
	}

	costiInversi := make(map[string]float64)

	CIL := 1 / CL

	costiInversi[myNodeID] = CIL

	CITot := CIL

	for nodeID, remoteCost := range CR {

		if remoteCost <= 0 {
			return nil, fmt.Errorf("remote cost for node %s must be greater than 0, got %f", nodeID, remoteCost)
		}

		CIR := 1 / remoteCost

		costiInversi[nodeID] = CIR

		CITot += CIR
	}

	pesi := make(map[string]uint)

	for nodeID, costoInverso := range costiInversi {
		PP := 100 * costoInverso / CITot
		pesi[nodeID] = uint(math.Round(PP))
	}

	return pesi, nil
}

func (strategy *CostBasedStrategy) calculateWeights() (map[string]map[string]uint, error) {
	logger := logging.Logger()

	myNodeID := _p2pHost.ID().String()

	weights := make(map[string]map[string]uint)

	var neighbours []string

	for _, p := range _p2pHost.Network().Peers() {
		if p == _p2pHost.ID() {
			continue
		}

		neighbours = append(
			neighbours,
			p.String(),
		)
	}

	if len(neighbours) != 2 {
		return nil, fmt.Errorf(
			"minimal test requires exactly 2 remote nodes, found %d",
			len(neighbours),
		)
	}

	funcs, err := strategy.faas.Names()
	if err != nil {
		return nil, fmt.Errorf("failed to check deployed functions: %w", err)
	}

	if len(funcs) == 0 {
		logger.Error("Strategy requires at least one deployed function, found none")
		return nil, fmt.Errorf("at least one function required, found %d", len(funcs))
	}

	for _, funcName := range funcs {

		weights[funcName] = make(map[string]uint)

		CL, CR := getMockCostsForFunction(
			funcName,
			neighbours,
		)

		pesi, err := calcolaPesi(
			myNodeID,
			CL,
			CR,
		)

		if err != nil {
			return nil, fmt.Errorf("calculating weights for function %s: %w", funcName, err)
		}

		for nodeID, peso := range pesi {
			weights[funcName][nodeID] = peso
		}

		logger.Infof("Function %s: local node %s, CL = %.2f, weight = %d", funcName, myNodeID, CL, weights[funcName][myNodeID])

		for nodeID, remoteCost := range CR {

			logger.Infof("Function %s: remote node %s, CR = %.2f, weight = %d", funcName, nodeID, remoteCost, weights[funcName][nodeID])
		}
	}

	debugWeightsNMS(weights)

	return weights, nil
}

func getMockCostsForFunction(funcName string, neighbours []string) (float64, map[string]float64) {
	CR := make(map[string]float64)

	var CL float64

	switch funcName {
	case "figlet":
		CL = 20.0
		for i, nodeID := range neighbours {

			if i == 0 {
				CR[nodeID] = 30.0
			} else {
				CR[nodeID] = 40.0
			}
		}

	case "shasum":
		CL = 35.0
		for i, nodeID := range neighbours {
			if i == 0 {
				CR[nodeID] = 25.0
			} else {
				CR[nodeID] = 50.0
			}
		}

	case "mlimage":
		CL = 120.0
		for i, nodeID := range neighbours {

			if i == 0 {
				CR[nodeID] = 80.0
			} else {
				CR[nodeID] = 150.0
			}
		}

	default:
		CL = 50.0
		for _, nodeID := range neighbours {
			CR[nodeID] = 60.0
		}
	}

	return CL, CR
}

func (strategy *CostBasedStrategy) setProxyWeights(weights map[string]map[string]uint) error {
	funcs, err := strategy.faas.WithTimeout()
	if err != nil {
		return fmt.Errorf("get function metadata: %w", err)
	}

	for _, timeout := range funcs {
		if timeout != nil {
			*timeout += 1000
		}
	}

	var neighbors []string

	neighborsPort := make(map[string]string)
	neighborsHost := make(map[string]string)

	for _, p := range _p2pHost.Network().Peers() {
		host, err := extractSingleIPv4(_p2pHost, p)
		if err != nil {
			return fmt.Errorf("failed to build neighbors information: %w", err)
		}

		peerID := p.String()

		neighbors = append(neighbors, peerID)
		neighborsHost[peerID] = host

		neighborsPort[peerID] = strconv.FormatUint(uint64(_config.HAProxyPort), 10)
	}

	data := struct {
		Now           string
		DFaaSNodeID   string
		Functions     map[string]*uint
		Neighbors     []string
		NeighborsPort map[string]string
		NeighborsHost map[string]string
		Weights       map[string]map[string]uint
		FaaSHost      string
		FaaSPort      uint
	}{
		Now:           time.Now().UTC().Format("2006-01-02 15:04:05 MST"),
		DFaaSNodeID:   _p2pHost.ID().String(),
		Functions:     funcs,
		Neighbors:     neighbors,
		NeighborsHost: neighborsHost,
		NeighborsPort: neighborsPort,
		Weights:       weights,
		FaaSHost:      _config.FaaSHost,
		FaaSPort:      _config.FaaSPort,
	}

	return strategy.hacfgupdater.UpdateHAConfig(data)
}
