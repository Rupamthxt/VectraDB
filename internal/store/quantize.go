package store

import "math"

type QuantizedVector struct {
	Data []int8
	Min  float32
	Max  float32
}

// Quantize compresses a float32 vector into an int8 vector
func Quantize(vec []float32) QuantizedVector {
	if len(vec) == 0 {
		return QuantizedVector{}
	}

	min, max := vec[0], vec[0]
	for _, v := range vec {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	if max == min {
		max = min + 1e-5
	}

	scale := 255.0 / (max - min)
	qVec := make([]int8, len(vec))
	for i, v := range vec {
		normalized := (v - min) * scale
		qVec[i] = int8(math.Round(float64(normalized)) - 128)
	}
	return QuantizedVector{
		Data: qVec,
		Min:  min,
		Max:  max,
	}
}

// Dequantize unpacks int8 vector back to float32 for precise math
func (q *QuantizedVector) Dequantize() []float32 {
	scale := (q.Max - q.Min) / 255.0
	fData := make([]float32, len(q.Data))
	for i, v := range q.Data {
		fData[i] = (float32(v)+128)*scale + q.Min
	}
	return fData
}

// DistQuantize calculates the approximate squared Euclidean distance between two vectors
// natively in int8. This is much faster as integer math is cheaper dompared to floating
// point math.
func DistQuantized(q1, q2 QuantizedVector) float32 {
	var dot, sq1, sq2 int32

	for i := 0; i < len(q1.Data); i++ {
		v1 := int32(q1.Data[i])
		v2 := int32(q2.Data[i])

		dot += v1 * v2
		sq1 += v1 * v1
		sq2 += v2 * v2
	}

	s1 := (q1.Max - q1.Min) / 255.0
	s2 := (q2.Max - q2.Min) / 255.0

	m1 := q1.Min + 128.0*s1
	m2 := q2.Min + 128.0*s2

	term1 := s1 * s1 * float32(sq1)
	term2 := s2 * s2 * float32(sq2)
	term3 := -2.0 * s1 * s2 * float32(dot)
	term4 := float32(len(q1.Data)) * (m1 - m2) * (m1 - m2)

	// Sum of elements for the cross terms
	var sum1, sum2 int32
	for i := 0; i < len(q1.Data); i++ {
		sum1 += int32(q1.Data[i])
		sum2 += int32(q2.Data[i])
	}

	term5 := 2.0 * s1 * (m1 - m2) * float32(sum1)
	term6 := -2.0 * s2 * (m1 - m2) * float32(sum2)

	return term1 + term2 + term3 + term4 + term5 + term6
}
