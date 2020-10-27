// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/v3/pkg/fileutil"

	"go.uber.org/zap"
)

//wal像是先建立一个临时文件，等操作完成时再重命名该文件（原子性）
//这个模块主要为了避免临时创建文件所产生的开销
//这个模块的代码会在后台启动一个协程时刻准备一个临时文件以供使用

// filePipeline pipelines allocating disk space
type filePipeline struct {
	lg *zap.Logger

	// dir to put files
	dir string
	// size of files to make, in bytes
	size int64
	// count number of files generated
	count int

	filec chan *fileutil.LockedFile
	errc  chan error
	donec chan struct{}
}

func newFilePipeline(lg *zap.Logger, dir string, fileSize int64) *filePipeline {
	if lg == nil {
		lg = zap.NewNop()
	}
	fp := &filePipeline{
		lg:    lg,
		dir:   dir,
		size:  fileSize,
		filec: make(chan *fileutil.LockedFile),
		errc:  make(chan error, 1),
		donec: make(chan struct{}),
	}
	go fp.run()
	return fp
}

//Open为啥这样设计
// Open returns a fresh file for writing. Rename the file before calling
// Open again or there will be file collisions.
func (fp *filePipeline) Open() (f *fileutil.LockedFile, err error) {
	select {
	case f = <-fp.filec:
	case err = <-fp.errc:
	}
	return f, err
}

func (fp *filePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc	//阻塞到fp.err里有内容？当run返回时关闭了fp.err后或者分配失败写入error之后，这里才会继续执行，为什么这样做？
}

//猜测的流程：外界调用Close，Close先关闭channel fp.donec,然后阻塞
//本模块里的run里面的select因为执行到了<-fp.donec，所以返回，在返回前关闭了fp.errc
//Close里的<-fp.errc得以继续执行，Close函数执行结束退出

//主要方法
func (fp *filePipeline) alloc() (f *fileutil.LockedFile, err error) {
	// count % 2 so this file isn't the same as the one last published
	fpath := filepath.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))	//0.tmp或1.tmp
	if f, err = fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return nil, err
	}
	//分配指定大小的空间
	if err = fileutil.Preallocate(f.File, fp.size, true); err != nil {
		fp.lg.Error("failed to preallocate space when creating a new WAL", zap.Int64("size", fp.size), zap.Error(err))
		f.Close()
		return nil, err
	}
	fp.count++
	return f, nil
}

func (fp *filePipeline) run() {
	defer close(fp.errc)
	for {
		f, err := fp.alloc()	//先创建临时文件并分配空间
		if err != nil {
			fp.errc <- err
			return
		}
		select {
		case fp.filec <- f:
		case <-fp.donec:	//Close关闭fp.donec了之后这里才有可能被执行到
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}
