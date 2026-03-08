package circulation

import (
	"context"
	"fmt"

	"github.com/lightningnetwork/lnd/lnrpc"

	"github.com/eddy/internal/lnd"
)

// hopFee computes the fee a hop charges for forwarding amtMsat.
func hopFee(baseMsat, rateMillionths int64, amtMsat int64) int64 {
	return baseMsat + (amtMsat*rateMillionths+999999)/1000000
}

// BuildRoute constructs a circular multi-hop route for the given cycle.
// All hops forward desiredMsat with zero fees. Non-participating nodes
// (without Eddy's interceptor) will reject the HTLC because LND enforces
// minimum fees. Participating nodes use RESUME_MODIFIED to waive fees.
//
// cycleNodes: [A, B, C, A] — first==last
// cycleChans: [ch_AB, ch_BC, ch_CA]
// selfID: the initiator's pubkey
// desiredAmtSat: the amount each channel balance should shift
func BuildRoute(
	ctx context.Context,
	client lnd.LNDClient,
	selfID string,
	cycleNodes []string,
	cycleChans []uint64,
	desiredAmtSat int64,
) (*lnrpc.Route, int64, error) {
	numHops := len(cycleChans)
	desiredMsat := desiredAmtSat * 1000

	info, err := client.GetInfo(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("getinfo: %w", err)
	}

	// Collect CLTV policies for each channel in the cycle.
	type hopPolicy struct {
		chanID        uint64
		pubKey        string
		timeLockDelta uint32
	}
	hops := make([]hopPolicy, numHops)

	for i := 0; i < numHops; i++ {
		edge, err := client.GetChanInfo(ctx, cycleChans[i])
		if err != nil {
			return nil, 0, fmt.Errorf("getchaninfo %d: %w", cycleChans[i], err)
		}
		var policy *lnrpc.RoutingPolicy
		if edge.Node1Pub == cycleNodes[i] {
			policy = edge.Node1Policy
		} else {
			policy = edge.Node2Policy
		}
		if policy == nil {
			return nil, 0, fmt.Errorf("no routing policy for channel %d from %s", cycleChans[i], cycleNodes[i][:16])
		}
		hops[i] = hopPolicy{
			chanID:        cycleChans[i],
			pubKey:        cycleNodes[i+1],
			timeLockDelta: policy.TimeLockDelta,
		}
	}

	// Build hops with zero fees. Every hop forwards desiredMsat so that
	// non-participating nodes (without Eddy's interceptor) can't forward
	// the HTLC — LND rejects insufficient-fee forwards.
	cltvExpiry := info.BlockHeight + 40 // final hop CLTV
	lnHops := make([]*lnrpc.Hop, numHops)

	lnHops[numHops-1] = &lnrpc.Hop{
		ChanId:           hops[numHops-1].chanID,
		Expiry:           cltvExpiry,
		AmtToForwardMsat: desiredMsat,
		PubKey:           hops[numHops-1].pubKey,
	}

	for i := numHops - 2; i >= 0; i-- {
		cltvExpiry += hops[i+1].timeLockDelta
		lnHops[i] = &lnrpc.Hop{
			ChanId:           hops[i].chanID,
			Expiry:           cltvExpiry,
			AmtToForwardMsat: desiredMsat,
			PubKey:           hops[i].pubKey,
		}
	}

	totalTimeLock := cltvExpiry + hops[0].timeLockDelta

	route := &lnrpc.Route{
		TotalTimeLock: totalTimeLock,
		TotalAmtMsat:  desiredMsat,
		TotalFeesMsat: 0,
		Hops:          lnHops,
	}

	return route, desiredAmtSat, nil
}
