package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type Movie struct {
	ID     int      `json:"id"`
	Title  string   `json:"title"`
	Year   int      `json:"year"`
	Genre  []string `json:"genre"`
	Rating float64  `json:"rating"`
}

func main() {
	// Open the movies CSV file
	moviesFile, err := os.Open("movies.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer moviesFile.Close()

	// Read the movies CSV file
	moviesReader := csv.NewReader(moviesFile)
	movieRecords, err := moviesReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// Extract headers
	movieHeaders := movieRecords[0]

	// Create a map to store movie data
	movies := make(map[int]Movie)

	// Process each row
	for _, row := range movieRecords[1:] {
		record := make(map[string]string)
		for i, value := range row {
			record[movieHeaders[i]] = value
		}

		// Extract and process the movie data
		movie, err := extractMovie(record)
		if err != nil {
			log.Printf("Skipping record due to error: %v", err)
			continue
		}
		movies[movie.ID] = movie
	}

	// Open the ratings CSV file
	ratingsFile, err := os.Open("ratings.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer ratingsFile.Close()

	// Read the ratings CSV file
	ratingsReader := csv.NewReader(ratingsFile)
	ratingRecords, err := ratingsReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// Process each rating row
	for _, row := range ratingRecords[1:] {
		movieId, err := strconv.Atoi(row[1])
		if err != nil {
			log.Printf("Skipping record due to error: %v", err)
			continue
		}
		rating, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			log.Printf("Skipping record due to error: %v", err)
			continue
		}

		// Update the movie rating
		if movie, exists := movies[movieId]; exists {
			movie.Rating = rating
			movies[movieId] = movie
		}
	}

	// Open the JSON file for writing
	jsonFile, err := os.Create("movies_with_ratings.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer jsonFile.Close()

	// Write each movie as a separate JSON object
	for _, movie := range movies {
		jsonBytes, err := json.Marshal(movie)
		if err != nil {
			log.Printf("Skipping record due to marshal error: %v", err)
			continue
		}
		_, err = jsonFile.Write(jsonBytes)
		if err != nil {
			log.Fatal(err)
		}
		_, err = jsonFile.WriteString("\n")
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("JSON data has been written to movies_with_ratings.txt")
}

func extractMovie(record map[string]string) (Movie, error) {
	title := record["title"]
	yearStr := title[len(title)-5 : len(title)-1]
	year, err := strconv.Atoi(yearStr)
	if err != nil {
		year = 2016 // default year if parsing fails
	}

	// Split the genres
	genres := strings.Split(record["genres"], "|")

	// Prepare the final Movie struct
	id, err := strconv.Atoi(record["movieId"])
	if err != nil {
		return Movie{}, err
	}

	movie := Movie{
		ID:     id,
		Title:  title,
		Year:   year,
		Genre:  genres,
		Rating: 0, // Initialize with default rating
	}

	return movie, nil
}
