package services

import (
	"github.com/greenplum-db/gpupgrade/idl"
)

func (h *Hub) Execute (request *idl.ExecuteRequest, stream idl.CliToHub_ExecuteServer) error {
	err := h.ExecuteInitTargetClusterSubStep(stream)
	if err != nil {
		return err
	}

	err = h.ExecuteShutdownClustersSubStep()
	if err != nil {
		return err
	}

	err = h.ExecuteUpgradeMasterSubStep(stream)
	if err != nil {
		return err
	}

	err = h.ExecuteCopyMasterSubStep()
	if err != nil {
		return err
	}

	err = h.ExecuteUpgradePrimariesSubStep()
	if err != nil {
		return err
	}

	err = h.ExecuteStartTargetClusterSubStep()
	return err
}
