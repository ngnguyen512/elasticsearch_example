package domain

type Movie struct {
	ID     int      `json:"id"`
	Title  string   `json:"title"`
	Year   int      `json:"year"`
	Genre  []string `json:"genre"`
	Rating float64  `json:"rating"`
}

type AggregationRequest struct {
	Size int                    `json:"size"`
	Aggs map[string]interface{} `json:"aggs"`
}

type Terms struct {
	Field string `json:"field"`
	Size  int    `json:"size"`
}

type Avg struct {
	Field string `json:"field"`
}

type AvgRatingPerGenreRequest struct {
	Terms *Terms                 `json:"terms"`
	Aggs  map[string]interface{} `json:"aggs"`
}

type Bucket struct {
	Key           string `json:"key"`
	DocumentCount int64  `json:"doc_count"`
	AvgRating     Value  `json:"avg_rating"`
}

type Value struct {
	Value float64 `json:"value"`
}

type AvgRatingPerGenreResponse struct {
	Buckets []Bucket `json:"buckets"`
}

type AggregationResponse struct {
	Aggregations struct {
		AvgRatingPerGenre AvgRatingPerGenreResponse `json:"avg_rating_per_genre"`
	} `json:"aggregations"`
}

type ClientKeyType string

const ClientKey ClientKeyType = "clientKey"
