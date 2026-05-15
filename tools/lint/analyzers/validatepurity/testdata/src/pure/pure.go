package pure

import "errors"

type Config struct {
	Name string
	Max  int
}

func (c *Config) Validate() error {
	if c.Name == "" {
		return errors.New("missing name")
	}
	tmp := c.Max
	tmp++
	_ = tmp
	return nil
}
