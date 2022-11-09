package raftconclave

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	log "github.com/sirupsen/logrus"
)

type RaftFSM struct {
	logger *log.Entry
}

func newRaftFSM(logger *log.Entry) (raftFSM *RaftFSM) {
	return &RaftFSM{
		logger: logger.WithFields(log.Fields{"Package": "reftconclave", "Module": "raftFSM"}),
	}
}

func (rf *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil
}

func (rf *RaftFSM) Apply(raftLog *raft.Log) any {
	logger := rf.logger.WithFields(log.Fields{"Function": "Apply"})
	switch raftLog.Type {
	case raft.LogCommand:
		logger.Errorf("Persisting logs not implemented. Data received '%s'", string(raftLog.Data))
		return fmt.Errorf("persisting logs not implemented")
	default:
		return fmt.Errorf("unknown raft log type: %#v", raftLog.Type)
	}
}

func (rf *RaftFSM) Restore(rc io.ReadCloser) error {
	logger := rf.logger.WithFields(log.Fields{"Function": "Restore"})
	logger.Warnf("Nothing to restore as we never store anything")
	return rc.Close()
}
