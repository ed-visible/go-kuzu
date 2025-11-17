package ryu

/*
#cgo darwin LDFLAGS: -lc++ -L${SRCDIR}/lib/dynamic/darwin -lryu -Wl,-rpath,${SRCDIR}/lib/dynamic/darwin
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/lib/dynamic/linux-amd64 -lryu -Wl,-rpath,${SRCDIR}/lib/dynamic/linux-amd64
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/lib/dynamic/linux-arm64 -lryu -Wl,-rpath,${SRCDIR}/lib/dynamic/linux-arm64
#cgo windows LDFLAGS: -L${SRCDIR}/lib/dynamic/windows -lryu_shared
#include "ryu.h"
*/
import "C"
