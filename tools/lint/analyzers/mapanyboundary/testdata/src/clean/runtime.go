package clean

type Stats struct {
	WriteCount uint64
	FlushCount uint64
}

func Build() Stats { return Stats{} }
