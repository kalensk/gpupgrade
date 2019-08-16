package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"syscall"

	"github.com/greenplum-db/gpupgrade/utils/daemon"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var listener net.Listener

func main() {
	var shouldDaemonize bool

	var RootCmd = &cobra.Command{
		Use:   "gpupgrade_watchdog ",
		Short: "Start the watchdog",
		Long:  `Start the watchdog`,
		Args:  cobra.MaximumNArgs(0), //no positional args allowed
		RunE: func(cmd *cobra.Command, args []string) error {
			return startWatchdog()
		},
	}

	RootCmd.AddCommand(&cobra.Command{
		Use:   "attach",
		Short: "Attach to a watchdog",
		Long:  "Attach to a watchdog",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sockPath := args[0]
			return attach(sockPath)
		},
	})

	daemon.MakeDaemonizable(RootCmd, &shouldDaemonize)

	err := RootCmd.Execute()
	if err != nil && err != daemon.ErrSuccessfullyDaemonized {
		log.Fatalf("%v", err)
	}
}

func attach(destSockPath string) error {
	localSockAddr := &net.UnixAddr{Name: "/tmp/attach.sock", Net: "unixgram"}
	uds, err := net.ListenUnixgram("unixgram", localSockAddr)
	if err != nil {
		return errors.Wrapf(err, "error listening on %s", localSockAddr)
	}
	defer uds.Close()
	defer os.Remove(localSockAddr.Name)

	destSockAddr := &net.UnixAddr{Name: destSockPath, Net: "unixgram"}
	_, _, err = uds.WriteMsgUnix([]byte("ping"), nil, destSockAddr)
	if err != nil {
		return errors.Wrap(err, "error writing")
	}

	buf := make([]byte, 64)
	oob := make([]byte, 64)

	_, oobn, flags, _, err := uds.ReadMsgUnix(buf, oob)
	if err != nil {
		return errors.Wrap(err, "failed to read UDS message")
	}

	log.Printf("buf: %v, oob: %v, flags: %v\n", buf, oob, flags)

	scms, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return errors.Wrap(err, "failed to parse OOB data")
	}

	if len(scms) != 1 {
		log.Panic("OMG")
	}

	scm := scms[0]
	fds, err := syscall.ParseUnixRights(&scm)
	if err != nil {
		return errors.Wrap(err, "failed to parse file descriptors...")
	}

	for _, fd := range fds {
		file := os.NewFile(uintptr(fd), "my received file")
		defer file.Close()

		for {
			n, err := file.Read(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, "failed to read from file")
			}

			log.Printf("read %v", string(buf[:n]))
		}
	}

	return err
}

func startWatchdog() error {
	sockAddr := &net.UnixAddr{Name: "/tmp/watchdog.sock", Net: "unixgram"}
	uds, err := net.ListenUnixgram("unixgram", sockAddr)
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}
	defer uds.Close()
	defer os.Remove(sockAddr.Name)

	buf := make([]byte, 64)

	_, remote, err := uds.ReadFromUnix(buf)
	if err != nil {
		return errors.Wrap(err, "error reading from client connection")
	}

	log.Printf("got %v from %v", buf, remote)

	reader, writer, err := os.Pipe()
	if err != nil {
		return errors.Wrap(err, "failed to create pipe")
	}
	defer writer.Close()

	_, _, err = uds.WriteMsgUnix([]byte("hello"), syscall.UnixRights(int(reader.Fd())), remote)
	if err != nil {
		return errors.Wrap(err, "failed to write to new UDS connection")
	}

	stderr, err := os.Create("watchdog.err")
	if err != nil {
		return errors.Wrap(err, "failed to create stderr spill file")
	}

	cmd := exec.Command("./mock_pg_upgrade")

	cmd.Stdout = writer
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(cmd.Stderr, "ERROR: watchdog failed to run due to: +%v", err.Error())
	}

	return nil
}
