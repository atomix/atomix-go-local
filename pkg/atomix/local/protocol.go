// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/stream"
	"time"
)

// NewProtocol returns an Atomix Protocol instance
func NewProtocol() rsm.Protocol {
	return &Protocol{}
}

// Protocol implements the Atomix protocol in process
type Protocol struct {
	clients map[rsm.PartitionID]*localClient
}

func (p *Protocol) Start(cluster *cluster.Cluster, registry rsm.Registry) error {
	clients := make(map[rsm.PartitionID]*localClient)
	for id := range cluster.Partitions() {
		partitionID := rsm.PartitionID(id)
		context := &localContext{
			partition: partitionID,
		}
		client := &localClient{
			state:   rsm.NewManager(cluster, registry, context),
			context: context,
			ch:      make(chan localRequest),
		}
		client.start()
		clients[partitionID] = client
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

type localContext struct {
	partition rsm.PartitionID
	index     rsm.Index
	timestamp time.Time
}

func (c *localContext) NodeID() string {
	return "local"
}

func (c *localContext) PartitionID() rsm.PartitionID {
	return c.partition
}

func (c *localContext) Index() rsm.Index {
	return c.index
}

func (c *localContext) Timestamp() time.Time {
	return c.timestamp
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
	state   *rsm.Manager
	context *localContext
	ch      chan localRequest
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

func (c *localClient) start() {
	go c.processRequests()
}

func (c *localClient) stop() {
	close(c.ch)
}

func (c *localClient) processRequests() {
	for request := range c.ch {
		if request.op == command {
			c.context.index++
			c.context.timestamp = time.Now()
			c.state.Command(request.input, request.stream)
		} else {
			c.state.Query(request.input, request.stream)
		}
	}
}

func (c *localClient) ExecuteCommand(ctx context.Context, input []byte, stream stream.WriteStream) error {
	c.ch <- localRequest{
		op:     command,
		input:  input,
		stream: stream,
	}
	return nil
}

func (c *localClient) ExecuteQuery(ctx context.Context, input []byte, stream stream.WriteStream) error {
	c.ch <- localRequest{
		op:     query,
		input:  input,
		stream: stream,
	}
	return nil
}
