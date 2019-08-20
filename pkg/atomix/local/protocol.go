package local

import (
	"context"
	"github.com/atomix/atomix-api/proto/atomix/controller"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"net"
	"time"
)

// NewLocalNode returns a new Atomix Node with a local protocol implementation
func NewLocalNode(lis net.Listener) *atomix.Node {
	return atomix.NewNode("local", &controller.PartitionConfig{}, NewLocalProtocol(), atomix.WithLocal(lis))
}

// NewLocalProtocol returns an Atomix LocalProtocol instance
func NewLocalProtocol() atomix.Protocol {
	return &LocalProtocol{}
}

// LocalProtocol implements the Atomix protocol in process
type LocalProtocol struct {
	atomix.Protocol
	stateMachine service.StateMachine
	client       *localClient
	context      *localContext
}

func (p *LocalProtocol) Start(cluster atomix.Cluster, registry *service.ServiceRegistry) error {
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

func (p *LocalProtocol) Client() service.Client {
	return p.client
}

func (p *LocalProtocol) Stop() error {
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
