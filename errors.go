package pgmq

import "errors"

var ErrNoRows = errors.New("pgmq: no rows in result set")

func wrapPostgresError(err error) error {
	return errors.New("postgres error: " + err.Error())
}
