package test

import (
	"github.com/farseer-go/fs"
	"github.com/farseer-go/queue"
	"testing"
)

func TestModule(t *testing.T) {
	fs.Initialize[queue.Module]("unit test")
	queue.Module{}.Shutdown()
}
