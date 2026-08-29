package rpc

import (
	"fmt"
	"net"
	"strings"

	"github.com/homura/core-bank/raft"
)

// ParsePeers turns "n1=host:port,n2=host:port" into a map and an id list.
//
// Validation here is not pedantry. Quorum size is derived from this list, so a
// one-character typo silently changes what the cluster believes it can survive.
// A duplicated id (n3 mistyped as n1) yields len(ids)=3 and majority=2 with only
// two real machines: the cluster reports healthy, tolerates zero failures, and
// the never-contacted node can later win an election and overwrite entries the
// first leader already acknowledged as committed.
func ParsePeers(s string) (map[raft.NodeID]string, []raft.NodeID, error) {
	addrs := make(map[raft.NodeID]string)
	var ids []raft.NodeID

	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Exactly one "=" per entry: "n1=host:port=oops" silently produced an
		// address of "host:port=oops".
		if strings.Count(part, "=") != 1 {
			return nil, nil, fmt.Errorf("bad peer %q: want exactly one '=', as id=host:port", part)
		}
		rawID, rawAddr, _ := strings.Cut(part, "=")

		// Trim each side separately; " n1 = addr " previously produced the id "n1 ".
		id := raft.NodeID(strings.TrimSpace(rawID))
		addr := strings.TrimSpace(rawAddr)

		if id == "" {
			return nil, nil, fmt.Errorf("bad peer %q: empty node id", part)
		}
		if addr == "" {
			return nil, nil, fmt.Errorf("bad peer %q: empty address", part)
		}
		if _, ok := addrs[id]; ok {
			return nil, nil, fmt.Errorf("duplicate node id %q: quorum size would be wrong, "+
				"and the missing node can later overwrite committed entries", id)
		}
		// A missing port is accepted by net.Dial in some forms but is never what
		// was meant here.
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, nil, fmt.Errorf("bad address %q for node %q: %w (want host:port)", addr, id, err)
		}

		addrs[id] = addr
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return nil, nil, fmt.Errorf("no peers given")
	}
	return addrs, ids, nil
}
