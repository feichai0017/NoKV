package wrap

type Backend struct{}

func (b *Backend) Foo(ctx int) error { return nil }

type Service struct {
	backend *Backend
}

func (s *Service) Foo(ctx int) error { // want `code_contract §7: Foo is a one-line forwarding wrapper.*`
	return s.backend.Foo(ctx)
}
