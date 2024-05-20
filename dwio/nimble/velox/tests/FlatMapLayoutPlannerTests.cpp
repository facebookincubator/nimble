/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dwio/nimble/velox/FlatMapLayoutPlanner.h"
#include "dwio/nimble/velox/tests/SchemaUtils.h"
#include "folly/Random.h"

using namespace ::facebook;

void addNamedTypes(
    const nimble::TypeBuilder& node,
    std::string prefix,
    std::vector<std::tuple<uint32_t, std::string>>& result) {
  switch (node.kind()) {
    case nimble::Kind::Scalar: {
      result.emplace_back(
          node.asScalar().scalarDescriptor().offset(), prefix + "s");
      break;
    }
    case nimble::Kind::Row: {
      auto& row = node.asRow();
      result.emplace_back(row.nullsDescriptor().offset(), prefix + "r");
      for (auto i = 0; i < row.childrenCount(); ++i) {
        addNamedTypes(
            row.childAt(i),
            fmt::format("{}r.{}({}).", prefix, row.nameAt(i), i),
            result);
      }
      break;
    }
    case nimble::Kind::Array: {
      auto& array = node.asArray();
      result.emplace_back(array.lengthsDescriptor().offset(), prefix + "a");
      addNamedTypes(
          array.elements(), folly::to<std::string>(prefix, "a."), result);
      break;
    }
    case nimble::Kind::ArrayWithOffsets: {
      auto& arrayWithOffsets = node.asArrayWithOffsets();
      result.emplace_back(
          arrayWithOffsets.offsetsDescriptor().offset(), prefix + "da.o");
      result.emplace_back(
          arrayWithOffsets.lengthsDescriptor().offset(), prefix + "da.l");
      addNamedTypes(
          arrayWithOffsets.elements(),
          folly::to<std::string>(prefix, "da.e:"),
          result);

      break;
    }
    case nimble::Kind::Map: {
      auto& map = node.asMap();
      result.emplace_back(map.lengthsDescriptor().offset(), prefix + "m");
      addNamedTypes(map.keys(), folly::to<std::string>(prefix, "m.k:"), result);
      addNamedTypes(
          map.values(), folly::to<std::string>(prefix, "m.v:"), result);
      break;
    }
    case nimble::Kind::SlidingWindowMap: {
      auto& map = node.asSlidingWindowMap();
      result.emplace_back(map.offsetsDescriptor().offset(), prefix + "sm.o");
      result.emplace_back(map.lengthsDescriptor().offset(), prefix + "sm.l");
      addNamedTypes(
          map.keys(), folly::to<std::string>(prefix, "sm.k:"), result);
      addNamedTypes(
          map.values(), folly::to<std::string>(prefix, "sm.v:"), result);
      break;
    }
    case nimble::Kind::FlatMap: {
      auto& flatmap = node.asFlatMap();
      result.emplace_back(flatmap.nullsDescriptor().offset(), prefix + "f");
      for (auto i = 0; i < flatmap.childrenCount(); ++i) {
        result.emplace_back(
            flatmap.inMapDescriptorAt(i).offset(),
            fmt::format("{}f.{}({}).im", prefix, flatmap.nameAt(i), i));
        addNamedTypes(
            flatmap.childAt(i),
            fmt::format("{}f.{}({}).", prefix, flatmap.nameAt(i), i),
            result);
      }
      break;
    }
  }
}

std::vector<std::tuple<uint32_t, std::string>> getNamedTypes(
    const nimble::TypeBuilder& root) {
  std::vector<std::tuple<uint32_t, std::string>> namedTypes;
  namedTypes.reserve(0); // To silence CLANGTIDY
  addNamedTypes(root, "", namedTypes);
  return namedTypes;
}

void testStreamLayout(
    std::mt19937& rng,
    nimble::FlatMapLayoutPlanner& planner,
    nimble::SchemaBuilder& builder,
    std::vector<nimble::Stream>&& streams,
    std::vector<std::string>&& expected) {
  std::random_shuffle(streams.begin(), streams.end());

  ASSERT_EQ(expected.size(), streams.size());
  streams = planner.getLayout(std::move(streams));
  ASSERT_EQ(expected.size(), streams.size());

  for (auto i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(expected[i], streams[i].content.front()) << "i = " << i;
  }

  // Now we test that planner can handle the case where less streams are
  // provided than the actual nodes in the schema.
  std::vector<nimble::Stream> streamSubset;
  std::vector<std::string> expectedSubset;
  streamSubset.reserve(streams.size());
  expectedSubset.reserve(streams.size());
  for (auto i = 0; i < streams.size(); ++i) {
    if (folly::Random::oneIn(2, rng)) {
      streamSubset.push_back(streams[i]);
      for (const auto& e : expected) {
        if (streamSubset.back().content.front() == e) {
          expectedSubset.push_back(e);
          break;
        }
      }
    }
  }

  std::random_shuffle(streamSubset.begin(), streamSubset.end());

  ASSERT_EQ(expectedSubset.size(), streamSubset.size());
  streamSubset = planner.getLayout(std::move(streamSubset));
  ASSERT_EQ(expectedSubset.size(), streamSubset.size());

  for (auto i = 0; i < expectedSubset.size(); ++i) {
    EXPECT_EQ(expectedSubset[i], streamSubset[i].content.front());
  }
}

TEST(FlatMapLayoutPlannerTests, ReorderFlatMap) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  nimble::SchemaBuilder builder;

  nimble::test::FlatMapChildAdder fm1;
  nimble::test::FlatMapChildAdder fm2;
  nimble::test::FlatMapChildAdder fm3;

  SCHEMA(
      builder,
      ROW({
          {"c1", TINYINT()},
          {"c2", FLATMAP(Int8, TINYINT(), fm1)},
          {"c3", ARRAY(TINYINT())},
          {"c4", FLATMAP(Int8, TINYINT(), fm2)},
          {"c5", TINYINT()},
          {"c6", FLATMAP(Int8, ARRAY(TINYINT()), fm3)},
      }));

  fm1.addChild("2");
  fm1.addChild("5");
  fm1.addChild("42");
  fm1.addChild("7");
  fm2.addChild("2");
  fm2.addChild("5");
  fm2.addChild("42");
  fm2.addChild("7");
  fm3.addChild("2");
  fm3.addChild("5");
  fm3.addChild("42");
  fm3.addChild("7");

  auto namedTypes = getNamedTypes(*builder.getRoot());

  nimble::FlatMapLayoutPlanner planner{
      [&]() { return builder.getRoot(); },
      {{1, {3, 42, 9, 2, 21}}, {5, {3, 2, 42, 21}}}};

  std::vector<nimble::Stream> streams;
  streams = planner.getLayout(std::move(streams));
  ASSERT_EQ(0, streams.size());

  streams.reserve(namedTypes.size());
  for (auto i = 0; i < namedTypes.size(); ++i) {
    streams.push_back(nimble::Stream{
        std::get<0>(namedTypes[i]), {std::get<1>(namedTypes[i])}});
  }

  std::vector<std::string> expected{
      // Row should always be first
      "r",
      // Followed by feature order
      "r.c2(1).f",
      "r.c2(1).f.42(2).im",
      "r.c2(1).f.42(2).s",
      "r.c2(1).f.2(0).im",
      "r.c2(1).f.2(0).s",
      "r.c6(5).f",
      "r.c6(5).f.2(0).im",
      "r.c6(5).f.2(0).a",
      "r.c6(5).f.2(0).a.s",
      "r.c6(5).f.42(2).im",
      "r.c6(5).f.42(2).a",
      "r.c6(5).f.42(2).a.s",
      // From here, streams follow schema order
      "r.c1(0).s",
      "r.c2(1).f.5(1).im",
      "r.c2(1).f.5(1).s",
      "r.c2(1).f.7(3).im",
      "r.c2(1).f.7(3).s",
      "r.c3(2).a",
      "r.c3(2).a.s",
      "r.c4(3).f",
      "r.c4(3).f.2(0).im",
      "r.c4(3).f.2(0).s",
      "r.c4(3).f.5(1).im",
      "r.c4(3).f.5(1).s",
      "r.c4(3).f.42(2).im",
      "r.c4(3).f.42(2).s",
      "r.c4(3).f.7(3).im",
      "r.c4(3).f.7(3).s",
      "r.c5(4).s",
      "r.c6(5).f.5(1).im",
      "r.c6(5).f.5(1).a",
      "r.c6(5).f.5(1).a.s",
      "r.c6(5).f.7(3).im",
      "r.c6(5).f.7(3).a",
      "r.c6(5).f.7(3).a.s",
  };

  testStreamLayout(
      rng, planner, builder, std::move(streams), std::move(expected));
}

TEST(FlatMapLayoutPlannerTests, ReorderFlatMapDynamicFeatures) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  nimble::SchemaBuilder builder;

  nimble::test::FlatMapChildAdder fm;

  SCHEMA(
      builder,
      ROW({
          {"c1", TINYINT()},
          {"c2", FLATMAP(Int8, TINYINT(), fm)},
          {"c3", ARRAY(TINYINT())},
      }));

  fm.addChild("2");
  fm.addChild("5");
  fm.addChild("42");
  fm.addChild("7");

  auto namedTypes = getNamedTypes(*builder.getRoot());

  nimble::FlatMapLayoutPlanner planner{
      [&]() { return builder.getRoot(); }, {{1, {3, 42, 9, 2, 21}}}};

  std::vector<nimble::Stream> streams;
  streams.reserve(namedTypes.size());
  for (auto i = 0; i < namedTypes.size(); ++i) {
    streams.push_back(nimble::Stream{
        std::get<0>(namedTypes[i]), {std::get<1>(namedTypes[i])}});
  }

  std::vector<std::string> expected{
      // Row should always be first
      "r",
      // Followed by feature order
      "r.c2(1).f",
      "r.c2(1).f.42(2).im",
      "r.c2(1).f.42(2).s",
      "r.c2(1).f.2(0).im",
      "r.c2(1).f.2(0).s",
      // From here, streams follow schema order
      "r.c1(0).s",
      "r.c2(1).f.5(1).im",
      "r.c2(1).f.5(1).s",
      "r.c2(1).f.7(3).im",
      "r.c2(1).f.7(3).s",
      "r.c3(2).a",
      "r.c3(2).a.s",
  };

  testStreamLayout(
      rng, planner, builder, std::move(streams), std::move(expected));

  fm.addChild("21");
  fm.addChild("3");
  fm.addChild("57");

  namedTypes = getNamedTypes(*builder.getRoot());

  streams.clear();
  streams.reserve(namedTypes.size());
  for (auto i = 0; i < namedTypes.size(); ++i) {
    streams.push_back(nimble::Stream{
        std::get<0>(namedTypes[i]), {std::get<1>(namedTypes[i])}});
  }

  expected = {
      // Row should always be first
      "r",
      // Followed by feature order
      "r.c2(1).f",
      "r.c2(1).f.3(5).im",
      "r.c2(1).f.3(5).s",
      "r.c2(1).f.42(2).im",
      "r.c2(1).f.42(2).s",
      "r.c2(1).f.2(0).im",
      "r.c2(1).f.2(0).s",
      "r.c2(1).f.21(4).im",
      "r.c2(1).f.21(4).s",
      // From here, streams follow schema order
      "r.c1(0).s",
      "r.c2(1).f.5(1).im",
      "r.c2(1).f.5(1).s",
      "r.c2(1).f.7(3).im",
      "r.c2(1).f.7(3).s",
      "r.c2(1).f.57(6).im",
      "r.c2(1).f.57(6).s",
      "r.c3(2).a",
      "r.c3(2).a.s",
  };

  testStreamLayout(
      rng, planner, builder, std::move(streams), std::move(expected));
}

TEST(FlatMapLayoutPlannerTests, IncompatibleOrdinals) {
  nimble::SchemaBuilder builder;

  nimble::test::FlatMapChildAdder fm1;
  nimble::test::FlatMapChildAdder fm2;
  nimble::test::FlatMapChildAdder fm3;

  SCHEMA(
      builder,
      ROW({
          {"c1", TINYINT()},
          {"c2", FLATMAP(Int8, TINYINT(), fm1)},
          {"c3", ARRAY(TINYINT())},
          {"c4", FLATMAP(Int8, TINYINT(), fm2)},
          {"c5", TINYINT()},
          {"c6", FLATMAP(Int8, ARRAY(TINYINT()), fm3)},
      }));

  fm1.addChild("2");
  fm1.addChild("5");
  fm1.addChild("42");
  fm1.addChild("7");
  fm2.addChild("2");
  fm2.addChild("5");

  nimble::FlatMapLayoutPlanner planner{
      [&]() { return builder.getRoot(); },
      {{1, {3, 42, 9, 2, 21}}, {2, {3, 2, 42, 21}}}};
  try {
    planner.getLayout({});
    FAIL() << "Factory should have failed.";
  } catch (const nimble::NimbleUserError& e) {
    EXPECT_THAT(
        e.what(), testing::HasSubstr("for feature ordering is not a flat map"));
  }
}

TEST(FlatMapLayoutPlannerTests, OrdinalOutOfRange) {
  nimble::SchemaBuilder builder;

  nimble::test::FlatMapChildAdder fm1;
  nimble::test::FlatMapChildAdder fm2;
  nimble::test::FlatMapChildAdder fm3;

  SCHEMA(
      builder,
      ROW({
          {"c1", TINYINT()},
          {"c2", FLATMAP(Int8, TINYINT(), fm1)},
          {"c3", ARRAY(TINYINT())},
          {"c4", FLATMAP(Int8, TINYINT(), fm2)},
          {"c5", TINYINT()},
          {"c6", FLATMAP(Int8, ARRAY(TINYINT()), fm3)},
      }));

  fm1.addChild("2");
  fm1.addChild("5");
  fm1.addChild("42");
  fm1.addChild("7");
  fm2.addChild("2");
  fm2.addChild("5");

  nimble::FlatMapLayoutPlanner planner{
      [&]() { return builder.getRoot(); },
      {{6, {3, 42, 9, 2, 21}}, {3, {3, 2, 42, 21}}}};
  try {
    planner.getLayout({});
    FAIL() << "Factory should have failed.";
  } catch (const nimble::NimbleUserError& e) {
    EXPECT_THAT(
        e.what(), testing::HasSubstr("for feature ordering is out of range"));
  }
}

TEST(FlatMapLayoutPlannerTests, IncompatibleSchema) {
  nimble::SchemaBuilder builder;

  SCHEMA(builder, MAP(TINYINT(), STRING()));

  nimble::FlatMapLayoutPlanner planner{
      [&]() { return builder.getRoot(); }, {{3, {3, 2, 42, 21}}}};
  try {
    planner.getLayout({});
    FAIL() << "Factory should have failed.";
  } catch (const nimble::NimbleInternalError& e) {
    EXPECT_THAT(
        e.what(),
        testing::HasSubstr(
            "Flat map layout planner requires row as the schema root"));
  }
}
