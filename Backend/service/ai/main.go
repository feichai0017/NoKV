package main

import (
	"context"
	"fmt"
	"log"

	"github.com/tmc/langchaingo/llms/ollama"
)

func main() {
	llm, err := ollama.New(
		ollama.WithModel("nomic-embed-text:v1.5"),
	)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	inputText := "The sky is blue because of Rayleigh scattering"
	result, err := llm.CreateEmbedding(ctx, []string{inputText})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%#v\n", result)
	fmt.Printf("%d\n", len(result[0]))
}
