/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package encoding

type Builder func() Encoder

type Encoder interface {
	Encode(data interface{}) ([]byte, error)
	Decode(data []byte) (interface{}, error)
}
