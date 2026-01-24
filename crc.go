package isledb

import "hash/crc32"

var crcTable = crc32.MakeTable(crc32.Castagnoli)
