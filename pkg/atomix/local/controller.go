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
	"fmt"
	"github.com/atomix/atomix-api/proto/atomix/controller"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"google.golang.org/grpc"
	"net"
	"sync"
)

// NewController returns a new local controller
func NewController(port int) *Controller {
	return &Controller{
		port:   port,
		groups: make(map[string]*partitionGroup),
	}
}

// Controller is a local controller instance
type Controller struct {
	port   int
	server *grpc.Server
	groups map[string]*partitionGroup
	mu     sync.RWMutex
}

// Start starts the local controller
func (c *Controller) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", c.port))
	if err != nil {
		return err
	}
	c.server = grpc.NewServer()
	controller.RegisterControllerServiceServer(c.server, &ControllerServer{c})
	go c.server.Serve(lis)
	return nil
}

// Stop stops the local controller
func (c *Controller) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.server.Stop()
	for _, group := range c.groups {
		group.Stop()
	}
}

// partitionGroup is a group of local partitions
type partitionGroup struct {
	ID         *controller.PartitionGroupId
	Spec       *controller.PartitionGroupSpec
	partitions []*partition
}

// Start starts the partition group
func (g *partitionGroup) Start() error {
	for _, partition := range g.partitions {
		if err := partition.Start(); err != nil {
			return err
		}
	}
	return nil
}

// Stop stops the partition group
func (g *partitionGroup) Stop() {
	for _, partition := range g.partitions {
		partition.Stop()
	}
}

// partition is a single local partition
type partition struct {
	Info  *controller.Partition
	nodes []*atomix.Node
}

// Start starts the partition
func (p *partition) Start() error {
	port := 5680
	for _, endpoint := range p.Info.Endpoints {
		var lis net.Listener
		var err error
		for {
			lis, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
			if _, ok := err.(*net.OpError); ok {
				port++
			} else {
				break
			}
		}

		node := NewNode(lis)
		go node.Start()

		endpoint.Host = "localhost"
		endpoint.Port = int32(port)
		p.nodes = append(p.nodes, node)
		port++
	}
	return nil
}

// Stop stops the partition
func (p *partition) Stop() {
	for _, node := range p.nodes {
		node.Stop()
	}
}

type ControllerServer struct {
	controller *Controller
}

func getNamespacedName(id *controller.PartitionGroupId) string {
	return fmt.Sprintf("%s:%s", id.Namespace, id.Name)
}

func (c *ControllerServer) CreatePartitionGroup(ctx context.Context, request *controller.CreatePartitionGroupRequest) (*controller.CreatePartitionGroupResponse, error) {
	c.controller.mu.Lock()
	defer c.controller.mu.Unlock()
	_, ok := c.controller.groups[getNamespacedName(request.ID)]
	if !ok {
		partitions := make([]*partition, request.Spec.Partitions)
		for i := 0; i < int(request.Spec.Partitions); i++ {
			endpoints := make([]*controller.PartitionEndpoint, request.Spec.PartitionSize)
			for i := 0; i < int(request.Spec.PartitionSize); i++ {
				endpoints[i] = &controller.PartitionEndpoint{}
			}

			partitions[i] = &partition{
				Info: &controller.Partition{
					PartitionID: int32(i) + 1,
					Endpoints:   endpoints,
				},
				nodes: make([]*atomix.Node, 0),
			}
		}

		group := &partitionGroup{
			ID:         request.ID,
			Spec:       request.Spec,
			partitions: partitions,
		}
		c.controller.groups[getNamespacedName(request.ID)] = group
		if err := group.Start(); err != nil {
			return nil, err
		}
	}
	return &controller.CreatePartitionGroupResponse{}, nil
}

func (c *ControllerServer) DeletePartitionGroup(ctx context.Context, request *controller.DeletePartitionGroupRequest) (*controller.DeletePartitionGroupResponse, error) {
	c.controller.mu.Lock()
	defer c.controller.mu.Unlock()
	group, ok := c.controller.groups[getNamespacedName(request.ID)]
	if ok {
		group.Stop()
		delete(c.controller.groups, getNamespacedName(request.ID))
	}
	return &controller.DeletePartitionGroupResponse{}, nil
}

func (c *ControllerServer) GetPartitionGroups(ctx context.Context, request *controller.GetPartitionGroupsRequest) (*controller.GetPartitionGroupsResponse, error) {
	c.controller.mu.RLock()
	defer c.controller.mu.RUnlock()

	groups := make([]*controller.PartitionGroup, 0, len(c.controller.groups))
	for _, group := range c.controller.groups {
		if request.ID != nil {
			if request.ID.Namespace != "" && request.ID.Namespace != group.ID.Namespace {
				continue
			}
			if request.ID.Name != "" && request.ID.Name != group.ID.Name {
				continue
			}
		}

		partitions := make([]*controller.Partition, len(group.partitions))
		for i, partition := range group.partitions {
			partitions[i] = partition.Info
		}
		groups = append(groups, &controller.PartitionGroup{
			ID:         group.ID,
			Spec:       group.Spec,
			Partitions: partitions,
		})
	}
	return &controller.GetPartitionGroupsResponse{
		Groups: groups,
	}, nil
}

func (c *ControllerServer) EnterElection(request *controller.PartitionElectionRequest, stream controller.ControllerService_EnterElectionServer) error {
	panic("not implemented")
}
