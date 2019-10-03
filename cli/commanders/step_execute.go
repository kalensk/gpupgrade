package commanders

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpupgrade/idl"
)

func Execute(client idl.CliToHubClient, verbose bool) error {
	stream, err := client.Execute(context.Background(), &idl.ExecuteRequest{})
	if err != nil {
		// TODO: Change the logging message?
		gplog.Error("ERROR - Unable to connect to hub")
		return err
	}

	for {
		var chunk *idl.Chunk
		chunk, err = stream.Recv()
		if err != nil {
			break
		}

		if chunk.Status != idl.StepStatus_UNKNOWN_STATUS {
			output := fmt.Sprintf("%s ", chunk.Step.String())
			os.Stdout.WriteString(output)
			output = fmt.Sprintf("%s\n", chunk.Status.String())
			os.Stdout.WriteString(output)
		}

		if verbose {
			if chunk.Type == idl.Chunk_STDOUT {
				os.Stdout.Write(chunk.Buffer)
			} else if chunk.Type == idl.Chunk_STDERR {
				os.Stderr.Write(chunk.Buffer)
			}
		}
	}

	if err != io.EOF {
		return err
	}

	return nil
}
