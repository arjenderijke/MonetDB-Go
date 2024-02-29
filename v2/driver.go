/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package monetdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

const DriverVersion = "2.1.0"

func init() {
	sql.Register("monetdb", &Driver{})
}

type Driver struct {
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return NewConnector(name)
}
