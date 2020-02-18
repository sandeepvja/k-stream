package stream

import (
	"github.com/pickme-go/k-stream/examples/example_1/encoders"
	kstream "github.com/pickme-go/k-stream/k-stream"
)

func initAccountDetailTable(builder *kstream.StreamBuilder) kstream.GlobalTable {

	return builder.GlobalTable(
		`account_detail`,
		encoders.KeyEncoder,
		encoders.AccountDetailsUpdatedEncoder,
		`account_detail_store`)
}