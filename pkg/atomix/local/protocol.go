// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"fmt"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/atomix/atomix-go-framework/pkg/atomix/stream"
)

// NewProtocol returns an Atomix Protocol instance
func NewProtocol() rsm.Protocol {
	return &Protocol{}
}

// Protocol implements the Atomix protocol in process
type Protocol struct {
	clients map[rsm.PartitionID]*localClient
}

func (p *Protocol) Start(cluster cluster.Cluster, registry *rsm.Registry) error {
	member, _ := cluster.Member()
	clients := make(map[rsm.PartitionID]*localClient)
	for _, partition := range cluster.Partitions() {
		client := &localClient{
			member: member,
			state:  rsm.NewStateMachine(registry),
			ch:     make(chan localRequest),
		}
		client.start()
		clients[rsm.PartitionID(partition.ID())] = client
	}
	p.clients = clients
	return nil
}

func (p *Protocol) Partition(partitionID rsm.PartitionID) rsm.Partition {
	return p.clients[partitionID]
}

func (p *Protocol) Partitions() []rsm.Partition {
	partitions := make([]rsm.Partition, 0, len(p.clients))
	for _, partition := range p.clients {
		partitions = append(partitions, partition)
	}
	return partitions
}

func (p *Protocol) Stop() error {
	for _, partition := range p.clients {
		partition.stop()
	}
	return nil
}

type operationType string

const (
	command operationType = "command"
	query   operationType = "query"
)

type localRequest struct {
	op     operationType
	input  []byte
	stream stream.WriteStream
}

type localClient struct {
	member *cluster.Member
	state  rsm.StateMachine
	ch     chan localRequest
}

func (c *localClient) MustLeader() bool {
	return false
}

func (c *localClient) IsLeader() bool {
	return false
}

func (c *localClient) Leader() string {
	return ""
}

func (c *localClient) Followers() []string {
	return nil
}

func (c *localClient) WatchConfig(ctx context.Context, ch chan<- rsm.PartitionConfig) error {
	go func() {
		ch <- rsm.PartitionConfig{
			Leader: fmt.Sprintf("%s:%d", c.member.Host, c.member.Port),
		}
	}()
	return nil
}

func (c *localClient) start() {
	go c.processRequests()
}

func (c *localClient) stop() {
	close(c.ch)
}

func (c *localClient) processRequests() {
	for request := range c.ch {
		if request.op == command {
			c.state.Command(request.input, request.stream)
		} else {
			c.state.Query(request.input, request.stream)
		}
	}
}

func (c *localClient) SyncCommand(ctx context.Context, input []byte, stream stream.WriteStream) error {
	c.ch <- localRequest{
		op:     command,
		input:  input,
		stream: stream,
	}
	return nil
}

func (c *localClient) SyncQuery(ctx context.Context, input []byte, stream stream.WriteStream) error {
	c.ch <- localRequest{
		op:     query,
		input:  input,
		stream: stream,
	}
	return nil
}

func (c *localClient) StaleQuery(ctx context.Context, input []byte, stream stream.WriteStream) error {
	c.ch <- localRequest{
		op:     query,
		input:  input,
		stream: stream,
	}
	return nil
}
