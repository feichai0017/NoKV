package markedok

type Backend struct{}

func (b *Backend) Foo(ctx int) error { return nil }

type Service struct {
	backend *Backend
}

// forwarding-ok: required gRPC adapter signature; remove when the upstream interface drops Foo (2026-Q4).
func (s *Service) Foo(ctx int) error {
	return s.backend.Foo(ctx)
}
