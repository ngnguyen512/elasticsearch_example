package logic

import (
	"ElasticSearchProject/internal/domain"
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
)

func AvgRatingPerGenreAgg(ctx context.Context) {
	client := ctx.Value(domain.ClientKey).(*elasticsearch.Client)
	var searchBuffer bytes.Buffer
	aggregRequest := domain.AggregationRequest{
		Size: 0,
		Aggs: map[string]interface{}{
			"avg_rating_per_genre": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "genre.keyword",
					"size":  10,
				},
				"aggs": map[string]interface{}{
					"avg_rating": map[string]interface{}{
						"avg": map[string]interface{}{
							"field": "rating",
						},
					},
				},
			},
		},
	}
	err := json.NewEncoder(&searchBuffer).Encode(aggregRequest)
	if err != nil {
		panic(err)
	}

	response, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex("movies"),
		client.Search.WithBody(&searchBuffer),
		client.Search.WithTrackTotalHits(true),
		client.Search.WithPretty(),
	)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()

	var aggregResponse = domain.AggregationResponse{}
	err = json.NewDecoder(response.Body).Decode(&aggregResponse)
	if err != nil {
		panic(err)
	}

	if len(aggregResponse.Aggregations.AvgRatingPerGenre.Buckets) > 0 {
		fmt.Printf("✅ Average Rating per Genre: \n")
		for _, bucket := range aggregResponse.Aggregations.AvgRatingPerGenre.Buckets {
			fmt.Printf("   🚀 %s = %.2f\n", bucket.Key, bucket.AvgRating.Value)
		}
	}

}
