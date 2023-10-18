package server

import (
	"log"
	"net"
	"syscall"

	"github.com/vivekimsit/quickdb/config"
	"github.com/vivekimsit/quickdb/core"
)

var con_clients int

func RunAsyncTCPServer() error {
	log.Println("starting an asynchronous TCP server on", config.Host, config.Port)

	max_clients := 20000
	con_clients = 0

	// Create kqueue instance
	kq, err := syscall.Kqueue()
	if err != nil {
		return err
	}

	// Create a new TCP socket
	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(serverFD)

	// Set the Socket to operate in non-blocking mode
	if err = syscall.SetNonblock(serverFD, true); err != nil {
		return err
	}

	// Bind the IP and port
	ip4 := net.ParseIP(config.Host).To4()
	addr := syscall.SockaddrInet4{Port: config.Port}
	copy(addr.Addr[:], ip4)

	if err = syscall.Bind(serverFD, &addr); err != nil {
		return err
	}

	// Start listening
	if err = syscall.Listen(serverFD, max_clients); err != nil {
		return err
	}

	// Create kevent
	events := make([]syscall.Kevent_t, max_clients)

	// Add server socket to kqueue
	_, err = syscall.Kevent(kq, []syscall.Kevent_t{{
		Ident:  uint64(serverFD),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}}, nil, nil)
	if err != nil {
		return err
	}

	for {
		// Wait for events
		nevents, err := syscall.Kevent(kq, nil, events[:], nil)
		if err != nil {
			continue
		}

		for i := 0; i < nevents; i++ {
			fd := int(events[i].Ident)

			if fd == serverFD {
				nfd, _, err := syscall.Accept(serverFD)
				if err != nil {
					log.Println("err", err)
					continue
				}
				con_clients++

				_, err = syscall.Kevent(kq, []syscall.Kevent_t{{
					Ident:  uint64(nfd),
					Filter: syscall.EVFILT_READ,
					Flags:  syscall.EV_ADD,
				}}, nil, nil)
				if err != nil {
					log.Fatal(err)
				}
			} else {
				comm := core.FDComm{Fd: fd}
				cmd, err := readCommand(comm)
				if err != nil {
					syscall.Close(fd)
					con_clients--
					continue
				}
				respond(cmd, comm)
			}
		}
	}
}
