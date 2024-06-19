package main

import (
	"ElasticSearchProject/internal/domain"
	"ElasticSearchProject/internal/elastic"
	"ElasticSearchProject/internal/elasticservice"
	"ElasticSearchProject/internal/logic"
	"ElasticSearchProject/internal/search"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type Movie struct {
	ID     int      `json:"id"`
	Title  string   `json:"title"`
	Year   int      `json:"year"`
	Genre  []string `json:"genre"`
	Rating float64  `json:"rating"`
}

func main() {
	// Initialize Elasticsearch client
	es, err := elastic.ConnectElasticsearch()
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client: %s", err)
	}

	_, err = es.Ping()

	// Load JSON data from file and bulk import
	err = bulkImport("movies_with_ratings.txt", "movies", es)
	if err != nil {
		log.Fatalf("Error during bulk import: %s", err)
	}

	fmt.Println("Bulk import completed.")

	ctx := context.WithValue(context.Background(), domain.ClientKey, es)

	// Find top drama movies after 2000
	err = search.FindTopMovieAfter2000(ctx, es)
	if err != nil {
		log.Fatalf("Error finding top drama movies: %s", err)
	}
	logic.AvgRatingPerGenreAgg(ctx)
	movie := domain.Movie{
		ID:     1389,
		Title:  "Jaws 3-D (1983)",
		Year:   1983,
		Genre:  []string{"Action", "Horror"},
		Rating: 2,
	}

	movieService := elasticservice.NewElasticService(es)

	err = movieService.CreateMovie(movie)
	if err != nil {
		log.Fatalf("Error creating movie: %s", err)
	}
	fmt.Println("Movie created successfully")

	// Delete a movie
	err = movieService.DeleteMovieById("1389")
	if err != nil {
		log.Fatalf("Error deleting movie: %s", err)
	}
	fmt.Println("Movie deleted successfully")

}

func bulkImport(filePath, indexName string, es *elasticsearch.Client) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var buffer bytes.Buffer
	count := 0
	batchSize := 1000

	for scanner.Scan() {
		line := scanner.Text()
		var movie Movie
		if err := json.Unmarshal([]byte(line), &movie); err != nil {
			log.Printf("Skipping record due to unmarshal error: %v", err)
			continue
		}

		meta := []byte(fmt.Sprintf(`{ "index" : { "_index" : "%s", "_id" : "%d" } }%s`, indexName, movie.ID, "\n"))
		data, err := json.Marshal(movie)
		if err != nil {
			log.Printf("Skipping record due to marshal error: %v", err)
			continue
		}

		buffer.Write(meta)
		buffer.Write(data)
		buffer.WriteString("\n")

		count++
		if count >= batchSize {
			if err := sendBulkRequest(es, &buffer); err != nil {
				return fmt.Errorf("error sending bulk request: %v", err)
			}
			count = 0
			buffer.Reset()
		}
	}

	if buffer.Len() > 0 {
		if err := sendBulkRequest(es, &buffer); err != nil {
			return fmt.Errorf("error sending bulk request: %v", err)
		}
	}

	return nil
}

func sendBulkRequest(es *elasticsearch.Client, buffer *bytes.Buffer) error {
	req := esapi.BulkRequest{
		Index: "movies",
		Body:  bytes.NewReader(buffer.Bytes()),
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return fmt.Errorf("error getting response: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error in response: %s", res.String())
	}

	return nil
}
