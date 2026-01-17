package http

type InsertRequest struct {
	ID     string         `json:"id"`
	Vector []float32      `json:"vector"`
	Data   map[string]any `json:"metadata"`
}

type SearchRequest struct {
	Vector []float32 `json:"vector"`
	TopK   int       `json:"k"`
}

type SearchResponse struct {
	Results []SearchResult `json:"results"`
}

type SearchResult struct {
	ID    string  `json:"id"`
	Score float32 `json:"score"`
	Data  any     `json:"metadata"`
}
