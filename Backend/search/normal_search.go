package search

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// SearchResult represents a single search result
type SearchResult struct {
	ID    string  `json:"id"`
	Title string  `json:"title"`
	Body  string  `json:"body"`
	Score float64 `json:"score"`
}

// Document represents a document to be indexed
type Document struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

// CreateIndex creates a new index with the specified name and mapping
func CreateIndex(esClient *elasticsearch.Client, indexName string) error {

	mapping := `{
		"mappings": {
			"properties": {
				"title": {"type": "text"},
				"body": {"type": "text"}
			}
		}
	}`

	req := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(mapping),
	}

	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		return fmt.Errorf("error creating index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error creating index: %s", res.String())
	}

	return nil
}

// IndexDocument indexes a new document or updates an existing one
func IndexDocument(esClient *elasticsearch.Client, indexName string, id string, doc Document) error {

	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("error marshaling document: %w", err)
	}

	req := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: id,
		Body:       strings.NewReader(string(body)),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		return fmt.Errorf("error indexing document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error indexing document: %s", res.String())
	}

	return nil
}

// DeleteIndex deletes the specified index
func DeleteIndex(esClient *elasticsearch.Client, indexName string) error {

	req := esapi.IndicesDeleteRequest{
		Index: []string{indexName},
	}

	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		return fmt.Errorf("error deleting index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error deleting index: %s", res.String())
	}

	return nil
}

// NormalSearch performs a full-text search on the specified index
func NormalSearch(esClient *elasticsearch.Client, query string, index string, size int) ([]SearchResult, error) {

	// Prepare the search query
	searchQuery := map[string]interface{}{
		"size": size,
		"query": map[string]interface{}{
			"multi_match": map[string]interface{}{
				"query":  query,
				"fields": []string{"title^2", "body"}, // Search in title and body fields, title has higher weight
			},
		},
		"highlight": map[string]interface{}{
			"fields": map[string]interface{}{
				"title": map[string]interface{}{},
				"body":  map[string]interface{}{},
			},
		},
	}

	// Convert query to JSON
	var buf strings.Builder
	if err := json.NewEncoder(&buf).Encode(searchQuery); err != nil {
		return nil, fmt.Errorf("error encoding query: %w", err)
	}

	// Perform the search request
	res, err := esClient.Search(
		esClient.Search.WithContext(context.Background()),
		esClient.Search.WithIndex(index),
		esClient.Search.WithBody(&buf),
		esClient.Search.WithTrackTotalHits(true),
		esClient.Search.WithPretty(),
	)
	if err != nil {
		return nil, fmt.Errorf("error performing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("search error: %s", res.String())
	}

	// Parse the response
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error parsing the response body: %w", err)
	}

	// Extract and format search results
	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})
	searchResults := make([]SearchResult, len(hits))

	for i, hit := range hits {
		source := hit.(map[string]interface{})["_source"].(map[string]interface{})
		searchResults[i] = SearchResult{
			ID:    hit.(map[string]interface{})["_id"].(string),
			Title: source["title"].(string),
			Body:  source["body"].(string),
			Score: hit.(map[string]interface{})["_score"].(float64),
		}
	}

	return searchResults, nil
}
