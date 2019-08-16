package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"

	"golang.org/x/sys/unix"

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
	destSockAddr := &net.UnixAddr{Name: destSockPath, Net: "unix"}
	uds, err := net.DialUnix("unix", nil, destSockAddr)
	if err != nil {
		return errors.Wrap(err, "error dialing")
	}
	defer uds.Close()

	_, _, err = uds.WriteMsgUnix([]byte("ping"), nil, nil)
	if err != nil {
		return errors.Wrap(err, "error writing")
	}

	buf := make([]byte, 64)
	oob := make([]byte, 64)

	bufn, oobn, flags, _, err := uds.ReadMsgUnix(buf, oob)
	if err != nil {
		return errors.Wrap(err, "failed to read UDS message")
	}

	log.Printf("buf: %v, oob: %v, flags: %v\n", buf[:bufn], oob[:oobn], flags)

	scms, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return errors.Wrap(err, "failed to parse OOB data")
	}

	if len(scms) != 1 {
		log.Panic("OMG")
	}

	scm := scms[0]
	fds, err := unix.ParseUnixRights(&scm)
	if err != nil {
		return errors.Wrap(err, "failed to parse file descriptors...")
	}

	done := make(chan error, len(fds))
	msgs := make([]chan []byte, len(fds))

	for i, fd := range fds {
		file := os.NewFile(uintptr(fd), "my received file")
		defer file.Close()

		buff := make([]byte, 64)
		msgs[i] = make(chan []byte)
		msg := msgs[i]

		go func() {
			for {
				n, err := file.Read(buff)
				if err != nil {
					if err == io.EOF {
						err = nil
					}
					done <- errors.Wrap(err, "failed to read from file")
					return
				}

				msg <- buff[:n]
			}
		}()
	}

	doneCount := 0
	for doneCount < 2 {
		select {
		case stdOut := <-msgs[0]:
			fmt.Print(string(stdOut))
		case stdErr := <-msgs[1]:
			_, err := fmt.Fprint(os.Stderr, string(stdErr))
			if err != nil {
				return errors.Wrap(err, "failed write to stderr")
			}
		case err := <-done:
			doneCount++
			if err != nil {
				return err
			}
		}
	}

	return err
}

func startWatchdog() error {
	sockAddr := &net.UnixAddr{Name: "/tmp/watchdog.sock", Net: "unix"}
	listener, err := net.ListenUnix("unix", sockAddr)
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}
	defer listener.Close()

	buf := make([]byte, 64)

	uds, err := listener.AcceptUnix()
	if err != nil {
		return errors.Wrap(err, "failed to accept")
	}

	bufn, _, err := uds.ReadFromUnix(buf)
	if err != nil {
		return errors.Wrap(err, "error reading from client connection")
	}

	log.Printf("got %v", buf[:bufn])

	outReader, outWriter, err := os.Pipe()
	if err != nil {
		return errors.Wrap(err, "failed to create out pipe")
	}
	defer outWriter.Close()

	errReader, errWriter, err := os.Pipe()
	if err != nil {
		return errors.Wrap(err, "failed to create err pipe")
	}
	defer errWriter.Close()

	_, _, err = uds.WriteMsgUnix([]byte("hello"), unix.UnixRights(int(outReader.Fd()), int(errReader.Fd())), nil)
	if err != nil {
		return errors.Wrap(err, "failed to write to new UDS connection")
	}

	cmd := exec.Command("./mock_pg_upgrade")

	cmd.Stdout = outWriter
	cmd.Stderr = errWriter
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(cmd.Stderr, "ERROR: watchdog failed to run due to: +%v", err.Error())
	}

	return nil
}
