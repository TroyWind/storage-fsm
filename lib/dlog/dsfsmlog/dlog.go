package dsfsmlog

import (
	"github.com/filecoin-project/storage-fsm/lib/util"
	"go.uber.org/zap"
)

var L *zap.Logger

func init() {
	L = util.GetXDebugLog("storage-fsm")
}
