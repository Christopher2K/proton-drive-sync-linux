//go:build linux

package vfs

import (
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"syscall"
)

var fuseOK = fuse.OK
var fuseENOENT = fuse.ENOENT

func fuseStatus(e syscall.Errno) fuse.Status { return fuse.Status(e) }

var ctx = &fuse.Context{}

var _ = nodefs.NewDefaultFile
