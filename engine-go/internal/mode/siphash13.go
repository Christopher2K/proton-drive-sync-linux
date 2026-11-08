package mode

import "math/bits"

// sip13 is a streaming SipHash-1-3 with zero keys — byte-for-byte
// compatible with Rust's std::collections::hash_map::DefaultHasher
// (SipHasher13, keys (0,0)), which root_key parity requires (see
// rustHashPath). Port of the reference SipHash with 1 compression round
// and 3 finalization rounds.
type sip13 struct {
	v0, v1, v2, v3 uint64
	// buf holds the partial 8-byte block, n the bytes buffered so far.
	buf [8]byte
	n   int
	// len is the total number of bytes written (mirrors Rust's
	// SipHasher13::length, folded into the final block).
	len uint64
}

func newSip13() *sip13 {
	// Initialization vector XORed with the (zero) key (standard SipHash
	// IVs: "somepseu" "dontnorm" "lygenera" "tedbytes").
	return &sip13{
		v0: 0x736f6d6570736575,
		v1: 0x646f72616e646f6d,
		v2: 0x6c7967656e657261,
		v3: 0x7465646279746573,
	}
}

func (s *sip13) sipRound() {
	s.v0 += s.v1
	s.v1 = bits.RotateLeft64(s.v1, 13)
	s.v1 ^= s.v0
	s.v0 = bits.RotateLeft64(s.v0, 32)
	s.v2 += s.v3
	s.v3 = bits.RotateLeft64(s.v3, 16)
	s.v3 ^= s.v2
	s.v0 += s.v3
	s.v3 = bits.RotateLeft64(s.v3, 21)
	s.v3 ^= s.v0
	s.v2 += s.v1
	s.v1 = bits.RotateLeft64(s.v1, 17)
	s.v1 ^= s.v2
	s.v2 = bits.RotateLeft64(s.v2, 32)
}

// absorb folds one 8-byte little-endian block into the state
// (c_rounds = 1 for SipHash-1-3).
func (s *sip13) absorb(m uint64) {
	s.v3 ^= m
	s.sipRound()
	s.v0 ^= m
}

// Write appends bytes to the hash (Rust Hasher::write).
func (s *sip13) Write(p []byte) {
	s.len += uint64(len(p))
	for _, b := range p {
		s.buf[s.n] = b
		s.n++
		if s.n == 8 {
			s.absorb(le64(s.buf[:]))
			s.n = 0
		}
	}
}

// writeU64 writes 8 little-endian bytes (Rust Hasher::write_usize on a
// 64-bit target).
func (s *sip13) writeU64(v uint64) {
	var tmp [8]byte
	for i := 0; i < 8; i++ {
		tmp[i] = byte(v >> (8 * i))
	}
	s.Write(tmp[:])
}

// Finish returns the 64-bit digest (Rust Hasher::finish): the final block
// is the buffered bytes with the total length mod 256 in the top byte.
func (s *sip13) Finish() uint64 {
	var tail uint64
	for i := 0; i < s.n; i++ {
		tail |= uint64(s.buf[i]) << (8 * i)
	}
	b := ((s.len & 0xff) << 56) | tail
	s.absorb(b)
	s.v2 ^= 0xff
	// d_rounds = 3 for SipHash-1-3.
	s.sipRound()
	s.sipRound()
	s.sipRound()
	return s.v0 ^ s.v1 ^ s.v2 ^ s.v3
}

func le64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}
