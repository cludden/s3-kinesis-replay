package validate

import validator "gopkg.in/go-playground/validator.v9"

// V is a shared validator value that can be reused
// throughout the application
var V *validator.Validate

func init() {
	V = validator.New()
}
