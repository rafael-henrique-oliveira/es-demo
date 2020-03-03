package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

const searchMatch = `
	"query": {
		"multi_match": {
		"query": %q,
		"fields": ["lastName^100", "firstName^10", "country", "title"],
		"operator": "and"
		}
	},
	"highlight": {
		"fields": {
		"lastName": { "number_of_fragments": 0 },
		"firstName": { "number_of_fragments": 0 },
		"country": { "number_of_fragments": 0 },
		"title": { "number_of_fragments": 0 }
		}
	},
	"size": 25,
	"sort": [{ "_score": "desc" }, { "_doc": "asc" }]`

var (
	listenAddr  string
	esAddresses string
)

// Person person struct
type Person struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Email     string `json:"email"`
	Country   string `json:"country"`
}

func main() {
	flag.StringVar(&listenAddr, "listen-addr", ":5000", "server listen address")
	flag.StringVar(&esAddresses, "es-addresses", "http://es01:9200,http://es02:9200",
		"elastic addresses")
	flag.Parse()

	logger := log.New(os.Stdout, "http: ", log.LstdFlags)

	done := make(chan bool, 1)
	quit := make(chan os.Signal, 1)

	signal.Notify(quit, os.Interrupt)

	es := newEsClient(logger, strings.Split(esAddresses, ","))
	err := bootstrap(es)
	if err != nil {
		panic(err)
	}

	server := newWebServer(logger, es)
	go gracefulShutdown(server, logger, quit, done)

	logger.Println("Server is ready to handle requests at", listenAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatalf("Could not listen on %s: %v\n", listenAddr, err)
	}

	<-done
	logger.Println("Server stopped")
}

func gracefulShutdown(server *http.Server, logger *log.Logger, quit <-chan os.Signal,
	done chan<- bool) {

	<-quit
	logger.Println("Server is shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server.SetKeepAlivesEnabled(false)
	if err := server.Shutdown(ctx); err != nil {
		logger.Fatalf("Could not gracefully shutdown the server: %v", err)
	}

	close(done)
}

func newWebServer(logger *log.Logger, es *elasticsearch.Client) *http.Server {
	router := http.NewServeMux()
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		logger.Println(r.Method, r.URL.Path, r.RemoteAddr, r.UserAgent())

		read, write := io.Pipe()

		go func() {
			defer write.Close()

			esInfo, err := es.Info()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			} else {
				defer esInfo.Body.Close()
				io.Copy(write, esInfo.Body)
			}
		}()

		io.Copy(w, read)
		w.WriteHeader(http.StatusOK)
	})

	router.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		logger.Println(r.Method, r.URL.Path, r.RemoteAddr, r.UserAgent())

		q := r.URL.Query().Get("q")

		read, write := io.Pipe()

		go func() {
			defer write.Close()

			res, err := es.Search(
				es.Search.WithContext(r.Context()),
				es.Search.WithIndex("people"),
				es.Search.WithBody(buildQuery(q)),
				es.Search.WithTrackTotalHits(true),
			)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			} else {
				defer res.Body.Close()
				io.Copy(write, res.Body)
			}
		}()

		io.Copy(w, read)
	})

	return &http.Server{
		Addr:         listenAddr,
		Handler:      router,
		ErrorLog:     logger,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}
}

func newEsClient(logger *log.Logger, addresses []string) *elasticsearch.Client {
	cfg := elasticsearch.Config{Addresses: addresses}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		logger.Println(err)
		panic(err)
	}

	return client
}

func buildQuery(query string) io.Reader {
	var b strings.Builder

	b.WriteString("{\n")
	b.WriteString(fmt.Sprintf(searchMatch, query))
	b.WriteString("\n}")

	return strings.NewReader(b.String())
}

func bootstrap(es *elasticsearch.Client) error {
	idx := "people"
	ctx := context.Background()
	_, err := esapi.IndicesDeleteRequest{Index: []string{idx}}.Do(ctx, es)
	if err != nil {
		return err
	}

	_, err2 := esapi.IndicesCreateRequest{Index: idx}.Do(ctx, es)
	if err2 != nil {
		return err2
	}

	people := make([]*Person, 4)
	people = append(people, &Person{
		ID:        "1",
		Title:     "Mr.",
		FirstName: "Marco",
		LastName:  "Franssen",
		Email:     "marco.franssen@elasticsearch.com",
		Country:   "The Netherlands",
	})
	people = append(people, &Person{
		ID:        "2",
		Title:     "Mr.",
		FirstName: "John",
		LastName:  "Doe",
		Email:     "john.doe@elasticsearch.com",
		Country:   "Neverland",
	})
	people = append(people, &Person{
		ID:        "3",
		Title:     "Mrs.",
		FirstName: "Jane",
		LastName:  "Doe",
		Email:     "jane.doe@golang.org",
		Country:   "Neverland",
	})
	people = append(people, &Person{
		ID:        "4",
		Title:     "Mr.",
		FirstName: "Rob",
		LastName:  "Pike",
		Email:     "rob.pike@golang.org",
		Country:   "Unknown",
	})

	for _, p := range people {
		payload, err := json.Marshal(p)
		if err != nil {
			return err
		}

		_, err3 := esapi.CreateRequest{
			Index:      idx,
			DocumentID: p.ID,
			Body:       bytes.NewReader(payload),
		}.Do(ctx, es)
		if err3 != nil {
			return err3
		}
	}

	return nil
}
