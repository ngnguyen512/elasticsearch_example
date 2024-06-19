package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"ElasticSearchProject/internal/elastic"

	"github.com/elastic/go-elasticsearch/v8"
)

func FindTopMovieAfter2000(ctx context.Context, client *elasticsearch.Client) error {
	var searchBuffer bytes.Buffer
	search := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": map[string]interface{}{
					"term": map[string]string{
						"genre.keyword": "Drama",
					},
				},
				"filter": []map[string]interface{}{
					{
						"range": map[string]interface{}{
							"year": map[string]int{
								"gt": 2000,
							},
						},
					},
					{
						"range": map[string]interface{}{
							"rating": map[string]float64{
								"gt": 4.0,
							},
						},
					},
				},
			},
		},
		"sort": []map[string]interface{}{
			{
				"rating": map[string]string{
					"order": "desc",
				},
			},
		},
	}
	err := json.NewEncoder(&searchBuffer).Encode(search)
	if err != nil {
		return fmt.Errorf("error encoding search query: %v", err)
	}

	response, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex("movies"),
		client.Search.WithBody(&searchBuffer),
		client.Search.WithTrackTotalHits(true),
		client.Search.WithPretty(),
	)
	if err != nil {
		return fmt.Errorf("error executing search query: %v", err)
	}
	defer response.Body.Close()

	var searchResponse elastic.SearchResponse
	err = json.NewDecoder(response.Body).Decode(&searchResponse)
	if err != nil {
		return fmt.Errorf("error decoding search response: %v", err)
	}

	if searchResponse.Hits.Total.Value > 0 {
		var movieTitles []string
		for _, hit := range searchResponse.Hits.Hits {
			movieTitles = append(movieTitles, hit.Source.Title)
		}
		fmt.Printf("✅ Top drama movies after 2000 with rating above 4: [%s] \n", strings.Join(movieTitles, ", "))
	} else {
		fmt.Println("No top drama movies found.")
	}

	return nil
}
