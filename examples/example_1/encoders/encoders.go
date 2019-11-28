package encoders

import (
	"github.com/pickme-go/k-stream/examples/example_1/events"
	"github.com/pickme-go/k-stream/k-stream/encoding"
)

var KeyEncoder = func() encoding.Encoder { return Int64Encoder{} }

var TransactionReceivedEncoder = func() encoding.Encoder { return TransactionEncoder{} }

var AccountCreditedEncoder = func() encoding.Encoder { return events.AccountCredited{} }

var AccountDebitedEncoder = func() encoding.Encoder { return events.AccountDebited{} }

var AccountDetailsUpdatedEncoder = func() encoding.Encoder { return events.AccountDetailsUpdated{} }

var CustomerProfileUpdatedEncoder = func() encoding.Encoder { return events.CustomerProfileUpdated{} }

var MessageEncoder = func() encoding.Encoder { return events.MessageCreated{} }
