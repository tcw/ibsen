package ext4

import (
	"hash/crc32"
	"testing"
)

func Benchmark(b *testing.B) {
	table := crc32.MakeTable(crc32.Castagnoli)

	bytes := []byte("ksdjflasdkjflsdjflasdfjlkasdjflalaksdjflasdkjflsdjflasdfjlkasdjflalaksdjflasdkjflsdjflasdfjlkasdjfla")
	checksum := crc32.Checksum(bytes, table)
	for i := 0; i < b.N; i++ {
		crc32.Update(checksum, table, bytes)
	}
}
