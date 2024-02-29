/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package monetdb

import (
	"context"
	"database/sql/driver"
	"time"
)

type Connector struct {
	name string
	cfg  Config
}

func NewConnector(name string, options ...connectorOption) (*Connector, error) {
	connector := &Connector{
		name: name,
	}
	connector.cfg = connector.cfg.DefaultConfig()
	for _, opt := range options {
		opt(&connector.cfg)
	}

	return connector, nil
}

func (c *Connector) Connect(context.Context) (driver.Conn, error) {
	return newConn(c.name, c.cfg)
}

func (c *Connector) Driver() driver.Driver {
	return &Driver{}
}

type connectorOption func(*Config)

func AutoCommitOption(autoCommit bool) connectorOption {
	return func(c *Config) {
		c.AutoCommit = autoCommit
	}
}

func ReplySizeOption(replySize int) connectorOption {
	return func(c *Config) {
		c.ReplySize = replySize
	}
}

func SizeHeaderOption(sizeHeader bool) connectorOption {
	return func(c *Config) {
		c.Sizeheader = sizeHeader
	}
}

func TimezoneOption(timezone *time.Location) connectorOption {
	return func(c *Config) {
		c.Timezone = timezone
	}
}
