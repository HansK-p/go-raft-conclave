package raftconclave

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	log "github.com/sirupsen/logrus"
)

type CfgPersistence struct {
	Folder string `yaml:"folder"`
}

type CfgMember struct {
	NodeID           string `yaml:"node_id"`
	RaftAddress      string `yaml:"raft_address"`
	DiscoveryAddress string `yaml:"discovery_address"`
}

type Configuration struct {
	Persistence CfgPersistence `yaml:"persistence"`
	Members     []CfgMember    `yaml:"members"`
	NodeID      string         `yaml:"node_id"`
}

type RaftConclave struct {
	logger *log.Entry
	ctx    context.Context
	wg     *sync.WaitGroup
	config *Configuration

	raft              *raft.Raft
	raftFSM           *RaftFSM
	boltStore         *raftboltdb.BoltStore
	raftAddress       string
	persistenceFolder string
}

func NewRaftConclave(logger *log.Entry, ctx context.Context, wg *sync.WaitGroup, config *Configuration) (raftConclave *RaftConclave, err error) {
	raftConclave = &RaftConclave{
		logger: logger.WithFields(log.Fields{"Package": "raftconclave", "Module": "RaftConclave", "MyNodeID": config.NodeID}),
		ctx:    ctx,
		wg:     wg,
		config: config,
	}

	for idx := range config.Members {
		if config.Members[idx].NodeID == config.NodeID {
			raftConclave.raftAddress = config.Members[idx].RaftAddress
		}
	}
	if raftConclave.raftAddress == "" {
		return nil, fmt.Errorf("unable to find raft address based on node ID '%s'", config.NodeID)
	}

	raftConclave.persistenceFolder = path.Join(config.Persistence.Folder, config.NodeID)
	return
}

func (raftConclave *RaftConclave) Run() (err error) {
	logger := raftConclave.logger.WithFields(log.Fields{"Function": "Schedule"})
	logger.Infof("Scheduling Raft")
	if err = os.MkdirAll(raftConclave.persistenceFolder, os.ModePerm); err != nil {
		return fmt.Errorf("when creating the persistence folder '%s': %w", raftConclave.persistenceFolder, err)
	}
	logger.Infof("Persistence folder '%s' created", raftConclave.persistenceFolder)

	if raftConclave.boltStore, err = raftboltdb.NewBoltStore(path.Join(raftConclave.persistenceFolder, "conclave.boltstore")); err != nil {
		return fmt.Errorf("when creating the bolt store in folder '%s': %w", raftConclave.persistenceFolder, err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftConclave.raftAddress)
	if err != nil {
		return fmt.Errorf("could not resolve address: %s", err)
	}

	transport, err := raft.NewTCPTransport(raftConclave.raftAddress, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return fmt.Errorf("could not create tcp transport: %s", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(path.Join(raftConclave.persistenceFolder, "snapshot"), 2, logger.Writer())
	if err != nil {
		return fmt.Errorf("could not create snapshot store: %s", err)
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(raftConclave.config.NodeID)

	raftConclave.raftFSM = newRaftFSM(raftConclave.logger)

	raftConclave.raft, err = raft.NewRaft(raftCfg, raftConclave.raftFSM, raftConclave.boltStore, raftConclave.boltStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("could not create raft instance: %s", err)
	}

	raftServers := []raft.Server{}
	for idx := range raftConclave.config.Members {
		logger := logger.WithFields(log.Fields{"Member": raftConclave.config.Members[idx]})
		logger.Infof("Add server and voter")
		raftServers = append(raftServers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(raftConclave.config.Members[idx].NodeID),
			Address:  raft.ServerAddress(raftConclave.config.Members[idx].DiscoveryAddress),
		})
	}

	raftConclave.raft.BootstrapCluster(raft.Configuration{Servers: raftServers})
	return nil
}

func (raftConclave *RaftConclave) IsLeader() (isLeader bool) {
	return raftConclave.raft.State() == raft.Leader
}

func (raftConclave *RaftConclave) Raft() (raftRaft *raft.Raft) {
	return raftConclave.raft
}
