//  Copyright (c) 2017-2021 VMware, Inc. or its affiliates
//  SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/upgrade"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
)

func (s *Server) UpgradeMirrors(ctx context.Context, req *idl.UpgradeMirrorsRequest) (*idl.UpgradeMirrorsReply, error) {
	gplog.Info("agent received request to upgrade mirrors")

	errs := make(chan error, len(req.PgOptions))
	var wg sync.WaitGroup

	for _, pgOpt := range req.PgOptions {
		pgOpt := pgOpt

		wg.Add(1)
		go func() {
			defer wg.Done()

			errs <- upgrade.UpgradeMirror(*pgOpt)
		}()
	}

	wg.Wait()
	close(errs)

	var err error
	for e := range errs {
		err = errorlist.Append(err, e)
	}

	return &idl.UpgradeMirrorsReply{}, err
}
