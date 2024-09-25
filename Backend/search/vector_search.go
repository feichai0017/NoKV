package search

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// CreateVectorIndex creates an index with vector field
func CreateVectorIndex(client *elasticsearch.Client, indexName string, vectorDimension int) error {
	mapping := fmt.Sprintf(`{
		"mappings": {
			"properties": {
				"vector_field": {
					"type": "dense_vector",
					"dims": %d
				},
				"text_field": {
					"type": "text"
				}
			}
		}
	}`, vectorDimension)

	req := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(mapping),
	}

	res, err := req.Do(context.Background(), client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error creating index: %s", res.String())
	}

	return nil
}

// InsertVectorData inserts vector data into the index
func InsertVectorData(client *elasticsearch.Client, indexName string, id string, vector []float32, text string) error {
	data := map[string]interface{}{
		"vector_field": vector,
		"text_field":   text,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: id,
		Body:       strings.NewReader(string(jsonData)),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error indexing document: %s", res.String())
	}

	return nil
}

// SearchVectors performs a vector search
func SearchVectors(client *elasticsearch.Client, indexName string, vector []float32, size int) ([]map[string]interface{}, error) {
	query := map[string]interface{}{
		"size": size,
		"query": map[string]interface{}{
			"script_score": map[string]interface{}{
				"query": map[string]interface{}{"match_all": map[string]interface{}{}},
				"script": map[string]interface{}{
					"source": "cosineSimilarity(params.query_vector, 'vector_field') + 1.0",
					"params": map[string]interface{}{"query_vector": vector},
				},
			},
		},
	}

	var buf strings.Builder
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, err
	}

	res, err := client.Search(
		client.Search.WithContext(context.Background()),
		client.Search.WithIndex(indexName),
		client.Search.WithBody(&buf),
		client.Search.WithTrackTotalHits(true),
		client.Search.WithPretty(),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error searching documents: %s", res.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	hits, _ := result["hits"].(map[string]interface{})
	hitsHits, _ := hits["hits"].([]interface{})

	var documents []map[string]interface{}
	for _, hit := range hitsHits {
		hitMap, _ := hit.(map[string]interface{})
		source, _ := hitMap["_source"].(map[string]interface{})
		documents = append(documents, source)
	}

	return documents, nil
}
