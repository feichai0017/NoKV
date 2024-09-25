package search

import (
	"cloud_distributed_storage/Backend/config"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
)

func NewESClient() (*elasticsearch.Client, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{config.ES_URL},
	})
	if err != nil {
		fmt.Println("Error creating Elasticsearch client:", err)
		return nil, err
	}
	return client, nil
}
