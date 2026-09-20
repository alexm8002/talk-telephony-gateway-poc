package media

func MuLawToPCM(u byte) int16 {
	u = ^u
	t := ((int(u)&0x0f)<<3 + 0x84) << ((uint(u) & 0x70) >> 4)
	if u&0x80 != 0 {
		return int16(0x84 - t)
	}
	return int16(t - 0x84)
}

func PCMToMuLaw(sample int16) byte {
	const bias = 0x84
	const clip = 32635
	s := int(sample)
	sign := byte(0)
	if s < 0 {
		s = -s
		sign = 0x80
	}
	if s > clip {
		s = clip
	}
	s += bias
	exponent := 7
	for mask := 0x4000; exponent > 0 && s&mask == 0; mask >>= 1 {
		exponent--
	}
	mantissa := (s >> (exponent + 3)) & 0x0f
	return ^(sign | byte(exponent<<4) | byte(mantissa))
}

func ALawToPCM(a byte) int16 {
	a ^= 0x55
	t := (int(a&0x0f) << 4) + 8
	seg := int((a & 0x70) >> 4)
	if seg >= 1 {
		t += 0x100
	}
	if seg > 1 {
		t <<= (seg - 1)
	}
	if a&0x80 == 0 {
		return int16(-t)
	}
	return int16(t)
}

func PCMToALaw(sample int16) byte {
	s := int(sample)
	sign := byte(0x80)
	if s < 0 {
		s = -s
		sign = 0x00
	}
	if s > 32767 {
		s = 32767
	}
	var exponent int
	var mantissa int
	if s < 256 {
		exponent = 0
		mantissa = s >> 4
	} else {
		exponent = 1
		for v := s >> 8; v > 1 && exponent < 7; v >>= 1 {
			exponent++
		}
		mantissa = (s >> (exponent + 3)) & 0x0f
	}
	return (sign | byte(exponent<<4) | byte(mantissa)) ^ 0x55
}

func DecodeG711(payload []byte, payloadType uint8) []int16 {
	out := make([]int16, len(payload))
	for i, b := range payload {
		if payloadType == 8 {
			out[i] = ALawToPCM(b)
		} else {
			out[i] = MuLawToPCM(b)
		}
	}
	return out
}

func EncodeG711(samples []int16, payloadType uint8) []byte {
	out := make([]byte, len(samples))
	for i, s := range samples {
		if payloadType == 8 {
			out[i] = PCMToALaw(s)
		} else {
			out[i] = PCMToMuLaw(s)
		}
	}
	return out
}

func Downsample48To8(in []int16) []int16 {
	if len(in) == 0 {
		return nil
	}
	out := make([]int16, 0, (len(in)+5)/6)
	for i := 0; i < len(in); i += 6 {
		end := i + 6
		if end > len(in) {
			end = len(in)
		}
		var sum int
		for _, v := range in[i:end] {
			sum += int(v)
		}
		out = append(out, int16(sum/(end-i)))
	}
	return out
}
