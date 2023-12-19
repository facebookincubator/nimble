include "dwio/alpha/encodings/Encodings.thrift"
include "thrift/annotation/thrift.thrift"

namespace cpp2 facebook.alpha.encodings
namespace java.swift com.facebook.alpha.encodings

struct ScalarNode {
  1: optional Encodings.Encoding encoding;
}

struct ArrayNode {
  1: optional Encodings.Encoding encoding;

  @thrift.Box
  2: optional Node elements;
}

struct ArrayWithOffsetsNode {
  1: optional Encodings.Encoding encoding;

  @thrift.Box
  2: optional Node offsets;

  @thrift.Box
  3: optional Node elements;
}

struct RowNode {
  1: optional Encodings.Encoding encoding;
  2: optional map<string, Node> children;
}

struct MapNode {
  1: optional Encodings.Encoding encoding;

  @thrift.Box
  2: optional Node keys;

  @thrift.Box
  3: optional Node values;
}

struct FlatMapNode {
  1: optional Encodings.Encoding encoding;
  2: optional map<string, Node> inMaps;
  3: optional map<string, Node> children;
}

union Node {
  1: ScalarNode scalarNode;
  2: ArrayNode arrayNode;
  3: RowNode rowNode;
  4: MapNode mapNode;
  5: FlatMapNode flatmapNode;
  6: ArrayWithOffsetsNode arrayWithOffsetsNode;
}
