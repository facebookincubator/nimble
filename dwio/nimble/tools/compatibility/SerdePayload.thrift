// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

package "facebook.com/dwio/nimble/tools/compatibility"

namespace cpp2 facebook.nimble.tools.compatibility

struct SerdePayload {
  // Versions this envelope contract, not the Nimble serialization format.
  1: i32 envelopeVersion;
  2: string schemaProfile;
  3: list<binary> payloads;
  4: i64 rowCount;
  5: string logicalChecksum;
  // Schema matching payloads: serializer schema for serialization payloads,
  // projected schema for projection payloads.
  6: binary payloadSchema;
  7: i64 fuzzerSeed;
  // Stable test-case name whose resolved selection is projectedSubfields.
  8: string projectionCaseId;
  // Concrete projection resolved from projectionCaseId. Serialization readers
  // use it to run Projector; projection validation uses it to verify test intent.
  9: list<string> projectedSubfields;
  10: string knobProfile;
  11: map<string, string> resolvedKnobs;
  // Per-feature writer encoding overrides; empty when selection is automatic.
  12: map<string, string> featureEncodingOverrides;
}
