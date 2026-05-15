package mutating

import "errors"

type Config struct {
	Name string
	Max  int
}

func (c *Config) Validate() error {
	if c.Name == "" {
		c.Name = "default" // want `code_contract §7: Validate mutates receiver c.*`
	}
	return nil
}

func (c *Config) ValidateLimit() error {
	if c.Max < 0 {
		return errors.New("negative")
	}
	c.Max++ // want `code_contract §7: ValidateLimit mutates receiver c.*`
	return nil
}
