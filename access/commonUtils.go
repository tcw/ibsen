package access

func Join(s ...[]byte) []byte {
	n := 0
	for _, v := range s {
		n += len(v)
	}

	b, i := make([]byte, n), 0
	for _, v := range s {
		i += copy(b[i:], v)
	}
	return b
}

func JoinSize(size int, s ...[]byte) []byte {
	b, i := make([]byte, size), 0
	for _, v := range s {
		i += copy(b[i:], v)
	}
	return b
}
