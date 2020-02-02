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
	"github.com/atomix/api/proto/atomix/controller"
	"github.com/atomix/go-framework/pkg/atomix"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/service"
	"github.com/atomix/go-framework/pkg/atomix/stream"
	"net"
	"time"
)

// NewNode returns a new Atomix Node with a local protocol implementation
func NewNode(lis net.Listener, registry *node.Registry, partitions []*controller.PartitionId) *atomix.Node {
	return atomix.NewNode("local", &controller.ClusterConfig{}, NewProtocol(registry, partitions), registry, atomix.WithLocal(lis))
}

// NewProtocol returns an Atomix Protocol instance
func NewProtocol(registry *node.Registry, partitions []*controller.PartitionId) node.Protocol {
	clients := make(map[int]*localClient)
	for _, partitionID := range partitions {
		context := &localContext{}
		stateMachine := node.NewPrimitiveStateMachine(registry, context)
		clients[int(partitionID.Partition)] = &localClient{
			stateMachine: stateMachine,
			context:      context,
			ch:           make(chan testRequest),
		}
	}
	return &Protocol{
		partitions: clients,
	}
}

// Protocol implements the Atomix protocol in process
type Protocol struct {
	partitions map[int]*localClient
}

func (p *Protocol) Start(cluster cluster.Cluster, registry *node.Registry) error {
	for _, partition := range p.partitions {
		partition.start()
	}
	return nil
}

func (p *Protocol) Partition(partitionID int) node.Partition {
	return p.partitions[partitionID]
}

func (p *Protocol) Partitions() []node.Partition {
	partitions := make([]node.Partition, 0, len(p.partitions))
	for _, partition := range p.partitions {
		partitions = append(partitions, partition)
	}
	return partitions
}

func (p *Protocol) Stop() error {
	for _, partition := range p.partitions {
		partition.stop()
	}
	return nil
}

type localContext struct {
	index     uint64
	timestamp time.Time
	operation service.OperationType
}

func (c *localContext) Node() string {
	return "local"
}

func (c *localContext) Index() uint64 {
	return c.index
}

func (c *localContext) Timestamp() time.Time {
	return c.timestamp
}

func (c *localContext) OperationType() service.OperationType {
	return c.operation
}

type testRequest struct {
	op     service.OperationType
	input  []byte
	stream stream.WriteStream
}

type localClient struct {
	stateMachine node.StateMachine
	context      *localContext
	ch           chan testRequest
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
		if request.op == service.OpTypeCommand {
			c.context.index++
			c.context.timestamp = time.Now()
			c.context.operation = service.OpTypeCommand
			c.stateMachine.Command(request.input, request.stream)
		} else {
			c.context.operation = service.OpTypeQuery
			c.stateMachine.Query(request.input, request.stream)
		}
	}
}

func (c *localClient) Write(ctx context.Context, input []byte, stream stream.WriteStream) error {
	c.ch <- testRequest{
		op:     service.OpTypeCommand,
		input:  input,
		stream: stream,
	}
	return nil
}

func (c *localClient) Read(ctx context.Context, input []byte, stream stream.WriteStream) error {
	c.ch <- testRequest{
		op:     service.OpTypeQuery,
		input:  input,
		stream: stream,
	}
	return nil
}
