package test

import (
	"cloud_distributed_storage/Backend/search"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestElasticsearch(t *testing.T) {
	// Create a new Elasticsearch client
	esClient, err := search.NewESClient()
	assert.NoError(t, err)

	// Test creating an index
	indexName := "test_index"
	err = search.CreateIndex(esClient, indexName)
	assert.NoError(t, err)

	// Test indexing a document
	doc := search.Document{
		Title: "Test Document",
		Body:  "This is a test document for Elasticsearch.",
	}
	err = search.IndexDocument(esClient, indexName, "1", doc)
	assert.NoError(t, err)

	// Test searching for documents
	results, err := search.NormalSearch(esClient, "test", indexName, 10)
	assert.NoError(t, err)
	assert.NotEmpty(t, results)

	// Clean up: delete the test index
	err = search.DeleteIndex(esClient, indexName)
	assert.NoError(t, err)
}
