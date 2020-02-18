package stream

import (
	"github.com/pickme-go/k-stream/examples/example_1/encoders"
	kstream "github.com/pickme-go/k-stream/k-stream"
)

func initCustomerProfileTable(builder *kstream.StreamBuilder) kstream.GlobalTable {

	return builder.GlobalTable(
		`customer_profile`,
		encoders.KeyEncoder,
		encoders.CustomerProfileUpdatedEncoder,
		`customer_profile_store`)
}