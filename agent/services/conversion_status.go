package services

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/greenplum-db/gpupgrade/hub/upgradestatus"
	pb "github.com/greenplum-db/gpupgrade/idl"
)

func (s *AgentServer) CheckConversionStatus(ctx context.Context, in *pb.CheckConversionStatusRequest) (*pb.CheckConversionStatusReply, error) {
	if len(in.GetSegments()) == 0 {
		return nil, errors.New("no segment information was passed to the agent")
	}
	format := "%s - DBID %d - CONTENT ID %d - PRIMARY - %s"

	var replies []string
	for _, segment := range in.GetSegments() {
		status := upgradestatus.SegmentConversionStatus(
			filepath.Join(s.conf.StateDir, upgradestatus.CONVERT_PRIMARIES, fmt.Sprintf("seg%d", segment.GetContent())),
			segment.GetDataDir(),
			s.executor,
		)

		// FIXME: we have status codes; why convert to strings?
		replies = append(replies, fmt.Sprintf(format, status.String(), segment.GetDbid(), segment.GetContent(), in.GetHostname()))
	}

	return &pb.CheckConversionStatusReply{
		Statuses: replies,
	}, nil
}
