package raftconclave

import "github.com/hashicorp/raft"

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(raft.SnapshotSink) error { return nil }
func (sn snapshotNoop) Release()                        {}
