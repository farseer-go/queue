package queue

import "github.com/farseer-go/fs/modules"

type Module struct {
}

func (module Module) DependsModule() []modules.FarseerModule {
	return []modules.FarseerModule{modules.FarseerKernelModule{}}
}

func (module Module) PreInitialize() {
	initConsumer()
}

func (module Module) Initialize() {
}

func (module Module) PostInitialize() {
}

func (module Module) Shutdown() {
}
