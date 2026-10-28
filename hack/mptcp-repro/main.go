// Repro: does an accepted socket inherit the listener's TCP_NOTSENT_LOWAT?
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const tcpIsMPTCP = 43

func read(name string, rc syscall.RawConn) {
	_ = rc.Control(func(fd uintptr) {
		lowat, err1 := unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_NOTSENT_LOWAT)
		mptcp, err2 := unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, tcpIsMPTCP)
		fmt.Printf("  %-9s TCP_NOTSENT_LOWAT=%d (err=%v)  TCP_IS_MPTCP=%d (err=%v)\n", name, lowat, err1, mptcp, err2)
	})
}

func main() {
	disable := len(os.Args) > 1 && os.Args[1] == "setmptcp-false"
	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_NOTSENT_LOWAT, 16384)
		}); err != nil {
			return err
		}
		return serr
	}}
	if disable {
		lc.SetMultipathTCP(false)
	}
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		panic(err)
	}
	defer c.Close()
	s, err := l.Accept()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	lrc, _ := l.(*net.TCPListener).SyscallConn()
	src, _ := s.(*net.TCPConn).SyscallConn()
	read("listener", lrc)
	read("accepted", src)
}
