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
	"github.com/atomix/atomix-api/proto/atomix/controller"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"net"
	"time"
)

// NewNode returns a new Atomix Node with a local protocol implementation
func NewNode(lis net.Listener) *atomix.Node {
	return atomix.NewNode("local", &controller.PartitionConfig{}, NewProtocol(), atomix.WithLocal(lis))
}

// NewProtocol returns an Atomix Protocol instance
func NewProtocol() atomix.Protocol {
	return &Protocol{}
}

// Protocol implements the Atomix protocol in process
type Protocol struct {
	atomix.Protocol
	stateMachine service.StateMachine
	client       *localClient
	context      *localContext
}

func (p *Protocol) Start(cluster atomix.Cluster, registry *service.ServiceRegistry) error {
	p.context = &localContext{}
	p.stateMachine = service.NewPrimitiveStateMachine(registry, p.context)
	p.client = &localClient{
		stateMachine: p.stateMachine,
		context:      p.context,
		ch:           make(chan testRequest),
	}
	p.client.start()
	return nil
}

func (p *Protocol) Client() service.Client {
	return p.client
}

func (p *Protocol) Stop() error {
	p.client.stop()
	return nil
}

type localContext struct {
	service.Context
	index     uint64
	timestamp time.Time
	operation service.OperationType
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

type localClient struct {
	stateMachine service.StateMachine
	context      *localContext
	ch           chan testRequest
}

type testRequest struct {
	op    service.OperationType
	input []byte
	ch    chan<- service.Output
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
			c.stateMachine.Command(request.input, request.ch)
		} else {
			c.context.operation = service.OpTypeQuery
			c.stateMachine.Query(request.input, request.ch)
		}
	}
}

func (c *localClient) Write(ctx context.Context, input []byte, ch chan<- service.Output) error {
	c.ch <- testRequest{
		op:    service.OpTypeCommand,
		input: input,
		ch:    ch,
	}
	return nil
}

func (c *localClient) Read(ctx context.Context, input []byte, ch chan<- service.Output) error {
	c.ch <- testRequest{
		op:    service.OpTypeQuery,
		input: input,
		ch:    ch,
	}
	return nil
}
