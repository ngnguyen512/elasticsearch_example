package elasticservice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	"ElasticSearchProject/internal/domain"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type ElasticService struct {
	Client *elasticsearch.Client
}

func NewElasticService(client *elasticsearch.Client) *ElasticService {
	return &ElasticService{Client: client}
}

func (s *ElasticService) CreateMovie(movie domain.Movie) error {
	data, err := json.Marshal(movie)
	if err != nil {
		return err
	}

	req := esapi.IndexRequest{
		Index:      "movies",
		DocumentID: fmt.Sprintf("%d", movie.ID),
		Body:       bytes.NewReader(data),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), s.Client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error indexing document ID=%d", movie.ID)
	}
	log.Printf("[%s] Document ID=%d indexed successfully", res.Status(), movie.ID)
	return nil
}

func (s *ElasticService) GetAllMovies() ([]domain.Movie, error) {
	var movies []domain.Movie
	req := esapi.SearchRequest{
		Index: []string{"movies"},
		Body:  bytes.NewReader([]byte(`{"query":{"match_all":{}}}`)),
	}

	res, err := req.Do(context.Background(), s.Client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error getting all movies: %s", res.String())
	}

	if err := json.NewDecoder(res.Body).Decode(&movies); err != nil {
		return nil, fmt.Errorf("error parsing the response body: %s", err)
	}
	return movies, nil
}

func (s *ElasticService) SearchMoviesByIds(ids []string) ([]domain.Movie, error) {
	var movies []domain.Movie
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"ids": map[string]interface{}{
				"values": ids,
			},
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, err
	}

	req := esapi.SearchRequest{
		Index: []string{"movies"},
		Body:  bytes.NewReader(buf.Bytes()),
	}

	res, err := req.Do(context.Background(), s.Client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error searching movies by ids: %s", res.String())
	}

	if err := json.NewDecoder(res.Body).Decode(&movies); err != nil {
		return nil, fmt.Errorf("error parsing the response body: %s", err)
	}
	return movies, nil
}

func (s *ElasticService) UpdateMovieById(id string, movie domain.Movie) error {
	data, err := json.Marshal(movie)
	if err != nil {
		return err
	}

	req := esapi.UpdateRequest{
		Index:      "movies",
		DocumentID: id,
		Body:       bytes.NewReader(data),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), s.Client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error updating document ID=%s", id)
	}
	log.Printf("[%s] Document ID=%s updated successfully", res.Status(), id)
	return nil
}

func (s *ElasticService) DeleteMovieById(id string) error {
	req := esapi.DeleteRequest{
		Index:      "movies",
		DocumentID: id,
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), s.Client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error deleting document ID=%s", id)
	}
	log.Printf("[%s] Document ID=%s deleted successfully", res.Status(), id)
	return nil
}
