package store

type Match struct {
	Index uint32
	Score float32
}

type MinHeap []Match

func (h *MinHeap) Len() int { return len(*h) }

func (h *MinHeap) Push(m Match) {
	*h = append(*h, m)
	h.up(len(*h) - 1)
}

func (h *MinHeap) Replace(m Match) {
	(*h)[0] = m
	h.down(0, len(*h))
}

func (h *MinHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || (*h)[j].Score >= (*h)[i].Score {
			break
		}
		(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
		j = i
	}
}

func (h *MinHeap) down(i0, n int) {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && (*h)[j2].Score < (*h)[j1].Score {
			j = j2
		}
		if (*h)[i].Score <= (*h)[j].Score {
			break
		}
		(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
		i = j
	}
}
