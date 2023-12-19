include "thrift/annotation/thrift.thrift"

namespace cpp2 facebook.alpha.encodings
namespace java.swift com.facebook.alpha.encodings

struct NumericTrivialEncodingParameters {
  1: optional CompressionParameters dataCompression;
}

struct StringTrivialEncodingParameters {
  1: optional CompressionParameters lengthCompression;
  2: optional CompressionParameters dataCompression;
  @thrift.Box
  3: optional EncodingParameters lengthsParameters;
}

struct BoolTrivialEncodingParameters {
  1: optional CompressionParameters dataCompression;
}

struct RleEncodingParameters {}

struct FixedBitWidthEncodingParameters {
  1: optional CompressionParameters dataCompression;
}

struct HuffmanEncodingParameters {}

struct SparseBoolEncodingParameters {
  1: optional CompressionParameters indexCompression;
}

struct VarintEncodingParameters {}

struct ConstantEncodingParameters {}

struct ChunkedEncodingParameters {}

struct DictionaryEncodingParameters {
  @thrift.Box
  1: optional EncodingParameters indicesParameters;
}

struct NullableEncodingParameters {
  @thrift.Box
  1: optional EncodingParameters nullsParameters;
  @thrift.Box
  2: optional EncodingParameters valuesParameters;
}

struct DeltaEncodingParameters {
  @thrift.Box
  1: optional EncodingParameters deltasParameters;
  @thrift.Box
  2: optional EncodingParameters restatementsParameters;
  @thrift.Box
  3: optional EncodingParameters isRestatementsParameters;
}

struct MainlyConstantEncodingParameters {
  @thrift.Box
  1: optional EncodingParameters isConstantParameters;
  @thrift.Box
  2: optional EncodingParameters otherValuesParameters;
}

struct SentinelEncodingParameters {
  @thrift.Box
  1: optional EncodingParameters valuesParameters;
}

union EncodingParameters {
  1: NumericTrivialEncodingParameters trivialNumeric;
  2: StringTrivialEncodingParameters trivialString;
  3: BoolTrivialEncodingParameters trivialBool;
  4: RleEncodingParameters rle;
  5: DictionaryEncodingParameters dictionary;
  6: FixedBitWidthEncodingParameters fixedBitWidth;
  7: HuffmanEncodingParameters huffman;
  8: NullableEncodingParameters nullable;
  9: SparseBoolEncodingParameters sparseBool;
  10: VarintEncodingParameters varint;
  11: DeltaEncodingParameters delta;
  12: ConstantEncodingParameters constant;
  13: MainlyConstantEncodingParameters mainlyConstant;
  14: SentinelEncodingParameters sentinel;
  15: ChunkedEncodingParameters chunked;
}

struct ZstdCompressionParameters {
  1: i16 level;
}

struct ZstrongCompressionParameters {
// TODO(felixh): fill out.
}

union CompressionParameters {
  1: ZstdCompressionParameters zstd;
  2: ZstrongCompressionParameters zstrong;
}

struct Encoding {
  1: EncodingParameters encoding;
  2: optional CompressionParameters compression;
}
