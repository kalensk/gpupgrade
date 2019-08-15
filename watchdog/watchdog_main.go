package main

import (
	"fmt"
	"github.com/greenplum-db/gpupgrade/utils/daemon"
	"github.com/spf13/cobra"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
)

const SockAddr = "/tmp/watchdog.sock"

var listener net.Listener

func main() {
	var shouldDaemonize bool

	var RootCmd = &cobra.Command{
		Use:   "gpupgrade_watchdog ",
		Short: "Start the watchdog",
		Long:  `Start the watchdog`,
		Args:  cobra.MaximumNArgs(0), //no positional args allowed
		RunE: func(cmd *cobra.Command, args []string) error {

			stdOut := os.Stdout
			stdErr := os.Stderr

			if shouldDaemonize {
				var err error
				stdOut, err = os.Create("watchdog.out")
				if err != nil {
					log.Fatalf("failed to open watchdog.out due to: %v", err)
				}

				stdErr, err = os.Create("watchdog.err")
				if err != nil {
					log.Fatalf("failed to open watchdog.err due to: %v", err)
				}

				daemon.Daemonize()
			}

			startWatchdog(stdOut, stdErr)

			return nil
		},
	}

	daemon.MakeDaemonizable(RootCmd, &shouldDaemonize)

	err := RootCmd.Execute()
	if err != nil && err != daemon.ErrSuccessfullyDaemonized {
		log.Fatal(err)
	}
}

func startServer() {
	if err := os.RemoveAll(SockAddr); err != nil {
		log.Fatal(err)
	}

	listener, err := net.Listen("unix", SockAddr)
	if err != nil {
		log.Fatal("listen error:", err)
	}

	defer listener.Close()
}


func startWatchdog(stdOut io.Writer, stdErr io.Writer) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error: ", err)
		}

	}



	cmd := exec.Command("mock_pg_upgrade")


	cmd.Stdout = stdOut
	cmd.Stderr = stdErr
	err := cmd.Run()
	if err != nil {
		fmt.Fprintf(cmd.Stderr, "ERROR: watchdog failed to run due to: +%v", err.Error())
	}
}


