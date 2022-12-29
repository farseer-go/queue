package test

import (
	"github.com/farseer-go/fs"
	"github.com/farseer-go/queue"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestModule(t *testing.T) {
	fs.Initialize[queue.Module]("unit test")
	assert.Panics(t, func() {
		fs.Exit(0)
	})
}
