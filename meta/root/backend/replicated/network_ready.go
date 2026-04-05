package replicated

import (
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	myraft "github.com/feichai0017/NoKV/raft"
)

func (d *NetworkDriver) drainLocked() ([]rootstorage.CommittedEvent, []myraft.Message, error) {
	if d.node == nil {
		return nil, nil, nil
	}
	var committed []rootstorage.CommittedEvent
	var outbound []myraft.Message
	for d.node.raw.HasReady() {
		rd := d.node.raw.Ready()
		readyCommitted, readyOutbound, err := d.applyReadyLocked(rd)
		if err != nil {
			return nil, nil, err
		}
		committed = append(committed, readyCommitted...)
		outbound = append(outbound, readyOutbound...)
		d.node.raw.Advance(rd)
	}
	if len(committed) > 0 {
		if err := d.adapter.appendCommitted(committed); err != nil {
			return nil, nil, err
		}
	}
	return committed, outbound, nil
}

func (d *NetworkDriver) applyReadyLocked(rd myraft.Ready) ([]rootstorage.CommittedEvent, []myraft.Message, error) {
	if err := d.persistReadyLocked(rd); err != nil {
		return nil, nil, err
	}
	committed, err := decodeCommittedEntries(rd.CommittedEntries)
	if err != nil {
		return nil, nil, err
	}
	return committed, rd.Messages, nil
}

func (d *NetworkDriver) persistReadyLocked(rd myraft.Ready) error {
	persistProtocol := false
	if !myraft.IsEmptyHardState(rd.HardState) {
		if err := d.node.storage.SetHardState(rd.HardState); err != nil {
			return err
		}
		persistProtocol = true
	}
	if !myraft.IsEmptySnap(rd.Snapshot) {
		if err := d.node.storage.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
		persistProtocol = true
	}
	if len(rd.Entries) > 0 {
		if err := d.node.storage.Append(rd.Entries); err != nil {
			return err
		}
		persistProtocol = true
	}
	if !persistProtocol {
		return nil
	}
	state, err := captureProtocolState(d.node.storage)
	if err != nil {
		return err
	}
	return saveProtocolState(d.workdir, state)
}

func decodeCommittedEntries(entries []myraft.Entry) ([]rootstorage.CommittedEvent, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	committed := make([]rootstorage.CommittedEvent, 0, len(entries))
	for _, entry := range entries {
		if entry.Type != myraft.EntryNormal || len(entry.Data) == 0 {
			continue
		}
		rec, err := unmarshalCommittedEvent(entry.Data)
		if err != nil {
			return nil, err
		}
		committed = append(committed, rec)
	}
	return committed, nil
}
