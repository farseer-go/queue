package queue

import (
	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs/modules"
	"time"
)

type Module struct {
}

func (module Module) DependsModule() []modules.FarseerModule {
	return nil
}

func (module Module) PreInitialize() {
	dicQueue = collections.NewDictionary[string, *queueManager]()
	MoveQueueInterval = time.Second * 5
}

func (module Module) Initialize() {
}

func (module Module) PostInitialize() {
}

func (module Module) Shutdown() {
}
