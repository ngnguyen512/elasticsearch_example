package elastic

import (
	"log"

	"github.com/elastic/go-elasticsearch/v8"
)

type SearchResponse struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			Source struct {
				Title string `json:"title"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func ConnectElasticsearch() (*elasticsearch.Client, error) {
	es, err := elasticsearch.NewClient(
		elasticsearch.Config{
			Username:               "elastic",
			Password:               "3Ispo3if9CHXKh4uP7Wt",
			Addresses:              []string{"https://localhost:9200"},
			CertificateFingerprint: "0659d8a6a7c51d5b8ffdb309be48f37adcc40e8390bf34a95b43868c86643152",
		},
	)
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client: %s", err)
		return nil, err
	}

	return es, nil
}
