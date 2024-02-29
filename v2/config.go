/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package monetdb

import (
	"time"

	"github.com/MonetDB/MonetDB-Go/v2/mapi"
)

type Config struct {
	AutoCommit bool
	ReplySize  int
	Sizeheader bool
	Timezone   *time.Location
}

func (cfg Config) DefaultConfig() Config {
	cfg.AutoCommit = true
	cfg.ReplySize = mapi.MAPI_ARRAY_SIZE
	cfg.Sizeheader = true
	cfg.Timezone = time.Local
	return cfg
}
