package store

import (
	"math"
)

func addToVector(a, b []float32) {
	for i := range a {
		a[i] += b[i]
	}
}

func divVector(a []float32, n float32) {
	if n == 0 {
		return
	}

	for i := range a {
		a[i] /= n
	}
}

func cosineSimilarity(a, b []float32) float32 {
	var dot, mag1, mag2 float32
	for i := range a {
		dot += a[i] * b[i]
		mag1 += a[i] * a[i]
		mag2 += b[i] * b[i]
	}
	if mag1 == 0 || mag2 == 0 {
		return 0
	}
	return dot / (float32(math.Sqrt(float64(mag1)))) * float32(math.Sqrt(float64(mag2)))

}
