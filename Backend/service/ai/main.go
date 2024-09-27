package main

import (
	"cloud_distributed_storage/Backend/search"
	document "cloud_distributed_storage/Backend/service/ai/handler"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/tmc/langchaingo/llms/ollama"
)

type OllamaEmbeddingModel struct {
	llm *ollama.LLM
}

func (m *OllamaEmbeddingModel) EmbedText(text string) ([]float32, error) {
	ctx := context.Background()
	response, err := m.llm.CreateEmbedding(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(response) == 0 || len(response[0]) == 0 {
		return nil, fmt.Errorf("no embeddings returned")
	}
	return response[0], nil
}

func main() {
	// 创建 Elasticsearch 客户端
	esClient, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// 创建 Ollama 嵌入模型
	llm, err := ollama.New(
		ollama.WithModel("nomic-embed-text:v1.5"),
	)
	if err != nil {
		log.Fatalf("Error creating Ollama model: %s", err)
	}

	embeddingModel := &OllamaEmbeddingModel{llm: llm}

	// 创建索引
	indexName := "documents"
	vectorDimension := 768 // nomic-embed-text:v1.5 的向量维度
	err = search.CreateVectorIndex(esClient, indexName, vectorDimension)
	if err != nil {
		log.Fatalf("Error creating index: %s", err)
	}

	// 创建处理器
	processor := document.NewProcessor(esClient, embeddingModel, indexName)

	// 处理文件夹中的所有文件
	folderPath := "path/to/your/documents/folder"
	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			log.Printf("Error opening file %s: %s", path, err)
			return nil
		}
		defer file.Close()

		err = processor.ProcessFile(path, file)
		if err != nil {
			log.Printf("Error processing file %s: %s", path, err)
			return nil
		}

		log.Printf("Successfully processed file: %s", path)
		return nil
	})

	if err != nil {
		log.Fatalf("Error walking through files: %s", err)
	}

	log.Println("All files processed successfully")
}
