package rootraft

import (
	"path/filepath"
	"testing"

	rootpkg "github.com/feichai0017/NoKV/meta/root"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/stretchr/testify/require"
)

func openTestClusterNode(t *testing.T, transport *MemoryTransport, workdir string, bootstrap bool, peers []Peer, nodeID uint64) *Node {
	t.Helper()
	node, err := OpenNode(Config{
		NodeID:    nodeID,
		Peers:     peers,
		Bootstrap: bootstrap,
		WorkDir:   workdir,
	}, Checkpoint{}, transport)
	require.NoError(t, err)
	transport.Register(nodeID, node)
	return node
}

func electLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	for i := 0; i < 32; i++ {
		for _, node := range nodes {
			require.NoError(t, node.Tick())
		}
		for _, node := range nodes {
			if node.Status().RaftState == myraft.StateLeader {
				return node
			}
		}
	}
	t.Fatal("meta/root/raft: leader election did not converge")
	return nil
}

func waitForClusterEpoch(t *testing.T, nodes []*Node, epoch uint64) {
	t.Helper()
	for i := 0; i < 64; i++ {
		ready := true
		for _, node := range nodes {
			if node.Current().ClusterEpoch != epoch {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		for _, node := range nodes {
			require.NoError(t, node.Tick())
		}
	}
	t.Fatalf("meta/root/raft: cluster epoch %d did not replicate", epoch)
}

func TestClusterElectsLeaderAndReplicatesDescriptor(t *testing.T) {
	transport := NewMemoryTransport()
	peers := []Peer{{ID: 1}, {ID: 2}, {ID: 3}}
	nodes := []*Node{
		openTestClusterNode(t, transport, filepath.Join(t.TempDir(), "n1"), true, peers, 1),
		openTestClusterNode(t, transport, filepath.Join(t.TempDir(), "n2"), true, peers, 2),
		openTestClusterNode(t, transport, filepath.Join(t.TempDir(), "n3"), true, peers, 3),
	}

	leader := electLeader(t, nodes)
	desc := testDescriptor(41, "a", "z")
	_, err := leader.ProposeEvent(rootpkg.RegionDescriptorPublished(desc))
	require.NoError(t, err)

	waitForClusterEpoch(t, nodes, 1)
	for _, node := range nodes {
		snap := node.Snapshot()
		require.Contains(t, snap.Descriptors, uint64(41))
		require.Equal(t, desc.Hash, snap.Descriptors[41].Hash)
	}
}

func TestClusterFollowerRestartRestoresAndCatchesUp(t *testing.T) {
	transport := NewMemoryTransport()
	base := t.TempDir()
	peers := []Peer{{ID: 1}, {ID: 2}, {ID: 3}}
	workdirs := []string{
		filepath.Join(base, "n1"),
		filepath.Join(base, "n2"),
		filepath.Join(base, "n3"),
	}
	nodes := []*Node{
		openTestClusterNode(t, transport, workdirs[0], true, peers, 1),
		openTestClusterNode(t, transport, workdirs[1], true, peers, 2),
		openTestClusterNode(t, transport, workdirs[2], true, peers, 3),
	}

	leader := electLeader(t, nodes)
	_, err := leader.ProposeEvent(rootpkg.RegionDescriptorPublished(testDescriptor(51, "", "m")))
	require.NoError(t, err)
	waitForClusterEpoch(t, nodes, 1)

	var followerIdx int
	for i, node := range nodes {
		if node.Status().RaftState != myraft.StateLeader {
			followerIdx = i
			break
		}
	}
	followerID := nodes[followerIdx].ID()
	transport.Unregister(followerID)

	reopened, err := OpenNode(Config{
		NodeID:  followerID,
		Peers:   peers,
		WorkDir: workdirs[followerIdx],
	}, Checkpoint{}, transport)
	require.NoError(t, err)
	transport.Register(followerID, reopened)
	nodes[followerIdx] = reopened

	_, err = leader.ProposeEvent(rootpkg.RegionDescriptorPublished(testDescriptor(52, "m", "z")))
	require.NoError(t, err)
	waitForClusterEpoch(t, nodes, 2)

	snap := reopened.Snapshot()
	require.Contains(t, snap.Descriptors, uint64(51))
	require.Contains(t, snap.Descriptors, uint64(52))
}
