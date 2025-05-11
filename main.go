package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/segmentio/kafka-go"
)

// ===== Article Struct =====
type Article struct {
	Title  string `json:"title"`
	URL    string `json:"url"`
	Source string `json:"source"`
}

// ===== Rate Limiter =====
type RateLimiter struct {
	requests map[string]int
	mu       sync.Mutex
	limit    int
	window   time.Duration
}

func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		requests: make(map[string]int),
		limit:    limit,
		window:   window,
	}
}

func (rl *RateLimiter) Limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rl.mu.Lock()
		defer rl.mu.Unlock()

		ip := strings.Split(r.RemoteAddr, ":")[0]
		if rl.requests[ip] >= rl.limit {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		rl.requests[ip]++
		time.AfterFunc(rl.window, func() {
			rl.mu.Lock()
			rl.requests[ip]--
			rl.mu.Unlock()
		})

		next.ServeHTTP(w, r)
	})
}

// ===== Scraper =====
func scrapeSource(url string, wg *sync.WaitGroup, ch chan<- Article) {
	defer wg.Done()

	res, err := http.Get(url)
	if err != nil {
		log.Println("Lỗi khi truy cập:", url, err)
		return
	}
	defer res.Body.Close()

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Println("Lỗi đọc nội dung:", err)
		return
	}

	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		title := strings.TrimSpace(s.Text())
		link, exists := s.Attr("href")

		if exists && title != "" {
			if !strings.HasPrefix(link, "/") && strings.Contains(link, "dantri.com.vn") {
				ch <- Article{
					Title:  title,
					URL:    link,
					Source: url,
				}
			}
		}
	})
}

func ScrapeNews(sources []string) []Article {
	var wg sync.WaitGroup
	ch := make(chan Article, 100)

	for _, source := range sources {
		wg.Add(1)
		go scrapeSource(source, &wg, ch)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var articles []Article
	for article := range ch {
		articles = append(articles, article)
	}

	return articles
}

// ===== Kafka Producer =====
var kafkaWriter *kafka.Writer

func InitProducer() {
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("kafka:9092"),
		Topic:    "news_topic",
		Balancer: &kafka.LeastBytes{},
	}
	log.Println("Kafka producer initialized")
}

func SendNewsToKafka(newsJson string) error {
	msg := kafka.Message{Value: []byte(newsJson)}
	return kafkaWriter.WriteMessages(context.Background(), msg)
}

func publishNewsToKafka(article Article) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"kafka:9093"},
		Topic:   "news_topic",
	})
	defer writer.Close()

	data, err := json.Marshal(article)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Value: data,
	}

	return writer.WriteMessages(context.Background(), msg)
}

// ===== Kafka Consumer =====
func StartConsumer(newsChan chan string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   "news_topic",
		GroupID: "news-consumer-group",
	})

	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				log.Println("Consumer error:", err)
				continue
			}
			fmt.Println("Received message:", string(m.Value))
			newsChan <- string(m.Value)
		}
	}()
}

func ConsumeNews() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9093"},
		Topic:   "news-updates",
		GroupID: "news-group",
	})

	for {
		m, err := reader.ReadMessage(nil)
		if err != nil {
			log.Fatal("failed to read message: ", err)
		}
		log.Printf("Consumed message: %s\n", string(m.Value))
	}
}

func ProduceArticle(article string) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"kafka:9093"},
		Topic:   "news-updates",
	})

	err := writer.WriteMessages(nil, kafka.Message{
		Value: []byte(article),
	})
	if err != nil {
		log.Fatal("failed to produce message: ", err)
	}

	writer.Close()
}

// ===== HTTP Handlers =====
func GetLatestArticles(w http.ResponseWriter, r *http.Request) {
	sources := []string{"https://vnexpress.net/", "https://dantri.com.vn/"}
	articles := ScrapeNews(sources)

	if len(articles) == 0 {
		log.Println("Không có bài viết nào")
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(articles)
}

func PublishNews(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		News Article `json:"news"`
	}
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil || payload.News.Title == "" {
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}

	err = publishNewsToKafka(payload.News)
	if err != nil {
		http.Error(w, fmt.Sprintf("Kafka publish error: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, "News published successfully")
}

func SetupRoutes() {
	http.Handle("/static/", http.StripPrefix("/static", http.FileServer(http.Dir("./public"))))
	http.HandleFunc("/articles", GetLatestArticles)
	http.HandleFunc("/publish", PublishNews)
}

// ===== MAIN =====
func main() {
	rateLimiter := NewRateLimiter(100, 1*time.Minute)
	SetupRoutes()
	InitProducer()
	newsChan := make(chan string)
	StartConsumer(newsChan)

	http.Handle("/", rateLimiter.Limit(http.DefaultServeMux))

	log.Println("Server running on http://localhost:8081")
	log.Fatal(http.ListenAndServe(":8081", nil))

}
