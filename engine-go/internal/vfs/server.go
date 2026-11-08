//go:build linux

package vfs

import (
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// Mount mounts fs (a PdsFs) at dir and returns the server once the
// kernel mount is up (WaitMount). The caller must server.Unmount() when
// done. The root inode always resolves: pathfs serves "" from GetAttr
// (AGENT.md gotcha).
func Mount(dir string, fs *PdsFs, debug bool) (*fuse.Server, error) {
	nfs := pathfs.NewPathNodeFs(fs, &pathfs.PathNodeFsOptions{Debug: debug})
	server, _, err := nodefs.Mount(dir, nfs.Root(), &fuse.MountOptions{Debug: debug}, &nodefs.Options{Debug: debug})
	if err != nil {
		return nil, err
	}
	go server.Serve()
	if err := server.WaitMount(); err != nil {
		server.Unmount()
		return nil, err
	}
	return server, nil
}
