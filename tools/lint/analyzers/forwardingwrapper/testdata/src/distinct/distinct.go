package distinct

type Backend struct {
	counter int
}

func (b *Backend) Tick() int { return b.counter }

type Service struct {
	backend *Backend
}

// Tick is not a forwarder: it adds bookkeeping on top.
func (s *Service) Tick() int {
	v := s.backend.Tick()
	return v + 1
}

// Latest is not a forwarder: it calls a differently-named backend method.
func (s *Service) Latest() int {
	return s.backend.Tick()
}
