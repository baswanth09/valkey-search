/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/response_generator.h"

#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/text/text_index.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/predicate.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

using testing::TestParamInfo;
using testing::ValuesIn;

class MockPredicate : public query::Predicate {
 public:
  explicit MockPredicate(query::PredicateType type) : query::Predicate(type) {}
  MOCK_METHOD(query::EvaluationResult, Evaluate, (query::Evaluator & evaluator),
              (override, const));
};

struct ResponseGeneratorTestCase {
  std::string test_name;
  data_model::AttributeDataType data_type;
  std::deque<std::string> external_id_neighbors;
  std::vector<TestReturnAttribute> return_attributes;
  absl::flat_hash_set<std::string> filter_identifiers;
  int filter_evaluate_not_match_index{-1};
  std::unordered_map<std::string, std::string> returned_records_map;
  absl::flat_hash_set<std::string> missing_keys;
  absl::flat_hash_set<std::string> expected_fetched_identifiers;
  std::vector<std::string> expected_neighbors;
  std::vector<std::unordered_map<std::string, std::string>> expected_contents;
};

class ResponseGeneratorTest
    : public ValkeySearchTestWithParam<ResponseGeneratorTestCase> {};

RecordsMap ToRecordsMap(
    const std::unordered_map<std::string, std::string> &record_map) {
  RecordsMap records_map;
  for (const auto &[key, value] : record_map) {
    records_map.emplace(key,
                        RecordsMapValue(vmsdk::MakeUniqueValkeyString(key),
                                        vmsdk::MakeUniqueValkeyString(value)));
  }
  return records_map;
}

TEST_P(ResponseGeneratorTest, ProcessNeighborsForReply) {
  auto &params = GetParam();
  ValkeyModuleCtx fake_ctx;

  std::deque<indexes::Neighbor> expected_neighbors;
  for (const auto &external_id : params.external_id_neighbors) {
    auto string_interned_external_id = StringInternStore::Intern(external_id);
    expected_neighbors.push_back(
        indexes::Neighbor(string_interned_external_id, 0));
  }
  std::vector<RecordsMap> expected_contents;
  expected_contents.reserve(params.expected_contents.size());
  for (const auto &expected_content : params.expected_contents) {
    expected_contents.push_back(ToRecordsMap(expected_content));
  }
  query::SearchParameters parameters(100000, nullptr);
  for (const auto &return_attribute : params.return_attributes) {
    parameters.return_attributes.push_back(
        {.identifier =
             vmsdk::MakeUniqueValkeyString(return_attribute.identifier),
         .alias = vmsdk::MakeUniqueValkeyString(return_attribute.alias)});
  }
  parameters.filter_parse_results.filter_identifiers =
      params.filter_identifiers;
  int filter_evaluate_cnt = -1;
  auto predicate =
      std::make_unique<MockPredicate>(query::PredicateType::kNumeric);
  EXPECT_CALL(*predicate, Evaluate(testing::_))
      .WillRepeatedly([&params, &filter_evaluate_cnt](
                          [[maybe_unused]] query::Evaluator &evaluator) {
        if (params.filter_evaluate_not_match_index == -1) {
          return query::EvaluationResult(true);
        }
        ++filter_evaluate_cnt;
        return query::EvaluationResult(filter_evaluate_cnt !=
                                       params.filter_evaluate_not_match_index);
      });

  parameters.filter_parse_results.root_predicate = std::move(predicate);
  parameters.attribute_alias = "some_attribute_name";

  MockAttributeDataType data_type;
  EXPECT_CALL(data_type, ToProto()).WillRepeatedly([&params]() {
    return params.data_type;
  });
  absl::flat_hash_set<absl::string_view> expected_fetched_identifiers;
  for (const auto &id : params.expected_fetched_identifiers) {
    expected_fetched_identifiers.insert(id);
  }
  for (const auto &neighbor : expected_neighbors) {
    EXPECT_CALL(
        data_type,
        FetchAllRecords(&fake_ctx, parameters.attribute_alias, testing::_,
                        absl::string_view(*neighbor.external_id),
                        expected_fetched_identifiers))
        .WillOnce([&params](
                      ValkeyModuleCtx *ctx,
                      const std::string &query_attribute_alias,
                      ValkeyModuleKey *open_key, absl::string_view key,
                      const absl::flat_hash_set<absl::string_view> &identifiers)
                      -> absl::StatusOr<RecordsMap> {
          if (params.missing_keys.contains(key)) {
            return absl::NotFoundError("not found");
          }
          auto returned_records_map = ToRecordsMap(params.returned_records_map);
          return std::move(returned_records_map);
        });
  }
  // The new API requires a send_response callback. For testing, we use an empty
  // callback since we're only testing the neighbor processing logic.
  auto send_response = [](ValkeyModuleCtx *, std::deque<indexes::Neighbor> &) {};
  ProcessNeighborsForReply(&fake_ctx, data_type, expected_neighbors, parameters,
                           parameters.attribute_alias, send_response);
  EXPECT_EQ(expected_neighbors.size(), params.expected_neighbors.size());
  for (size_t i = 0; i < params.expected_neighbors.size(); ++i) {
    EXPECT_EQ(std::string(*expected_neighbors[i].external_id),
              params.expected_neighbors[i]);
    EXPECT_EQ(ToStringMap(expected_neighbors[i].attribute_contents.value()),
              ToStringMap(expected_contents[i]));
  }
}

TEST_F(ResponseGeneratorTest, ProcessNeighborsForReplyContentLimits) {
  ValkeyModuleCtx fake_ctx;

  // Set up a small content size limit for testing
  const size_t test_size_limit = 100;
  VMSDK_EXPECT_OK(
      options::GetMaxSearchResultRecordSize().SetValue(test_size_limit));

  // Set up a small content fields limit for testing
  const size_t test_fields_limit = 2;
  VMSDK_EXPECT_OK(
      options::GetMaxSearchResultFieldsCount().SetValue(test_fields_limit));

  // Create neighbors with different content sizes and field counts
  std::deque<indexes::Neighbor> neighbors;
  auto small_external_id = StringInternStore::Intern("small_content_id");
  auto large_external_id = StringInternStore::Intern("large_content_id");
  auto many_fields_id = StringInternStore::Intern("many_fields_id");

  neighbors.push_back(indexes::Neighbor(small_external_id, 0));
  neighbors.push_back(indexes::Neighbor(large_external_id, 0));
  neighbors.push_back(indexes::Neighbor(many_fields_id, 0));

  // Set up parameters
  query::SearchParameters parameters(100000, nullptr);
  parameters.return_attributes.push_back(
      {.identifier = vmsdk::MakeUniqueValkeyString("content"),
       .alias = vmsdk::MakeUniqueValkeyString("content_alias")});
  parameters.return_attributes.push_back(
      {.identifier = vmsdk::MakeUniqueValkeyString("field1"),
       .alias = vmsdk::MakeUniqueValkeyString("field1_alias")});
  parameters.return_attributes.push_back(
      {.identifier = vmsdk::MakeUniqueValkeyString("field2"),
       .alias = vmsdk::MakeUniqueValkeyString("field2_alias")});
  parameters.attribute_alias = "test_attribute";

  // Mock data type
  MockAttributeDataType data_type;
  EXPECT_CALL(data_type, ToProto()).WillRepeatedly([]() {
    return data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH;
  });

  // Mock FetchAllRecords to return different sized content
  EXPECT_CALL(data_type, FetchAllRecords(
                             &fake_ctx, parameters.attribute_alias, testing::_,
                             absl::string_view("small_content_id"), testing::_))
      .WillOnce([](ValkeyModuleCtx *ctx,
                   const std::string &query_attribute_alias,
                   ValkeyModuleKey *open_key, absl::string_view key,
                   const absl::flat_hash_set<absl::string_view> &identifiers)
                    -> absl::StatusOr<RecordsMap> {
        // Return small content (within both size and field limits)
        RecordsMap small_content;
        small_content.emplace(
            "content", RecordsMapValue(vmsdk::MakeUniqueValkeyString("content"),
                                       vmsdk::MakeUniqueValkeyString("small")));
        small_content.emplace(
            "field1", RecordsMapValue(vmsdk::MakeUniqueValkeyString("field1"),
                                      vmsdk::MakeUniqueValkeyString("value1")));
        return small_content;
      });

  EXPECT_CALL(data_type, FetchAllRecords(
                             &fake_ctx, parameters.attribute_alias, testing::_,
                             absl::string_view("large_content_id"), testing::_))
      .WillOnce([test_size_limit](
                    ValkeyModuleCtx *ctx,
                    const std::string &query_attribute_alias,
                    ValkeyModuleKey *open_key, absl::string_view key,
                    const absl::flat_hash_set<absl::string_view> &identifiers)
                    -> absl::StatusOr<RecordsMap> {
        // Return large content (exceeds size limit)
        RecordsMap large_content;
        std::string large_value(test_size_limit + 10,
                                'x');  // Exceed the size limit by 10 bytes
        large_content.emplace(
            "content",
            RecordsMapValue(vmsdk::MakeUniqueValkeyString("content"),
                            vmsdk::MakeUniqueValkeyString(large_value)));
        return large_content;
      });

  EXPECT_CALL(data_type,
              FetchAllRecords(&fake_ctx, parameters.attribute_alias, testing::_,
                              absl::string_view("many_fields_id"), testing::_))
      .WillOnce([](ValkeyModuleCtx *ctx,
                   const std::string &query_attribute_alias,
                   ValkeyModuleKey *open_key, absl::string_view key,
                   const absl::flat_hash_set<absl::string_view> &identifiers)
                    -> absl::StatusOr<RecordsMap> {
        // Return content with many fields (exceeds field count limit)
        RecordsMap many_fields_content;
        many_fields_content.emplace(
            "content", RecordsMapValue(vmsdk::MakeUniqueValkeyString("content"),
                                       vmsdk::MakeUniqueValkeyString("data")));
        many_fields_content.emplace(
            "field1", RecordsMapValue(vmsdk::MakeUniqueValkeyString("field1"),
                                      vmsdk::MakeUniqueValkeyString("value1")));
        many_fields_content.emplace(
            "field2", RecordsMapValue(vmsdk::MakeUniqueValkeyString("field2"),
                                      vmsdk::MakeUniqueValkeyString("value2")));
        return many_fields_content;
      });

  // The new API requires a send_response callback. For testing, we use an empty
  // callback since we're only testing the content limit logic.
  auto send_response = [](ValkeyModuleCtx *, std::deque<indexes::Neighbor> &) {};
  ProcessNeighborsForReply(&fake_ctx, data_type, neighbors, parameters,
                           parameters.attribute_alias, send_response);

  // Verify that only the neighbor with small content remains
  // (both large content and many fields neighbors should be filtered out)
  EXPECT_EQ(neighbors.size(), 1);
  EXPECT_EQ(std::string(*neighbors[0].external_id), "small_content_id");
  EXPECT_TRUE(neighbors[0].attribute_contents.has_value());

  // Verify the content is correct
  auto content_map = ToStringMap(neighbors[0].attribute_contents.value());
  EXPECT_EQ(content_map["content"], "small");
  EXPECT_EQ(content_map["field1"], "value1");
  EXPECT_EQ(content_map.size(), 2);

  // Verify that the metric was incremented correctly
  // Should be incremented by 2: once for large content, once for many fields
  EXPECT_EQ(Metrics::GetStats().query_result_record_dropped_cnt, 2);
}

INSTANTIATE_TEST_SUITE_P(
    ResponseGeneratorTests, ResponseGeneratorTest,
    ValuesIn<ResponseGeneratorTestCase>(
        {{
             .test_name = "json_with_filter_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}},
                                   {{"id1", "id1_value"}}},
         },

         {
             .test_name =
                 "json_with_filter_with_return_missing_key_external_id1",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {"external_id1"},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id2",
                 },
             .expected_contents =
                 {
                     {{"id1", "id1_value"}},
                 },
         },
         {
             .test_name = "json_filter_not_match_first_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = 0,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}}},
         },
         {
             .test_name = "json_filter_not_match_second_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = 1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                 },
             .expected_contents = {{{"id1", "id1_value"}}},
         },
         {
             .test_name = "json_no_filter_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .returned_records_map =
                 {
                     {"id1", "id1_value"},
                 },
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1"},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}},
                                   {{"id1", "id1_value"}}},
         },
         {
             .test_name = "json_no_filter_no_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .returned_records_map =
                 {
                     {std::string(kJsonRootElementQuery), "id1_value"},
                 },
             .missing_keys = {},
             .expected_fetched_identifiers = {std::string(
                 kJsonRootElementQuery)},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents =
                 {{{std::string(kJsonRootElementQuery), "id1_value"}},
                  {{std::string(kJsonRootElementQuery), "id1_value"}}},
         },
         {
             .test_name = "json_with_filter_with_no_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{std::string(kJsonRootElementQuery),
                                       "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers =
                 {std::string(kJsonRootElementQuery), "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents =
                 {{{std::string(kJsonRootElementQuery), "id1_value"}},
                  {{std::string(kJsonRootElementQuery), "id1_value"}}},
         },
         {
             .test_name = "hash_with_filter_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}},
                                   {{"id1", "id1_value"}}},
         },
         {
             .test_name =
                 "hash_with_filter_with_return_missing_key_external_id1",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {"external_id2"},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                 },
             .expected_contents =
                 {
                     {{"id1", "id1_value"}},
                 },
         },
         {
             .test_name = "hash_filter_not_match_first_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = 0,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}}},
         },
         {
             .test_name = "hash_filter_not_match_second_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = 1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                 },
             .expected_contents = {{{"id1", "id1_value"}}},
         },
         {
             .test_name = "hash_no_filter_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .returned_records_map =
                 {
                     {"id1", "id1_value"},
                 },
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1"},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}},
                                   {{"id1", "id1_value"}}},
         },
         {
             .test_name = "hash_with_filter_with_no_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}, {"id2", "id2_value"}},
                                   {{"id1", "id1_value"},
                                    {"id2", "id2_value"}}},
         }}),
    [](const TestParamInfo<ResponseGeneratorTestCase> &info) {
      return info.param.test_name;
    });

// Tests for ContainsTextPredicate function
class ContainsTextPredicateTest : public ValkeySearchTest {};

TEST_F(ContainsTextPredicateTest, NullPredicateReturnsFalse) {
  EXPECT_FALSE(query::ContainsTextPredicate(nullptr));
}

TEST_F(ContainsTextPredicateTest, TagPredicateReturnsFalse) {
  data_model::TagIndex tag_index_proto;
  tag_index_proto.set_separator(",");
  auto tag_index = std::make_shared<indexes::Tag>(tag_index_proto);

  absl::flat_hash_set<absl::string_view> tags = {"tag1"};
  auto predicate = std::make_unique<query::TagPredicate>(
      tag_index.get(), "alias", "identifier", "tag1", tags);

  EXPECT_FALSE(query::ContainsTextPredicate(predicate.get()));
}

TEST_F(ContainsTextPredicateTest, NumericPredicateReturnsFalse) {
  data_model::NumericIndex numeric_index_proto;
  auto numeric_index = std::make_shared<indexes::Numeric>(numeric_index_proto);

  auto predicate = std::make_unique<query::NumericPredicate>(
      numeric_index.get(), "alias", "identifier", 0.0, true, 100.0, true);

  EXPECT_FALSE(query::ContainsTextPredicate(predicate.get()));
}

TEST_F(ContainsTextPredicateTest, TermPredicateReturnsTrue) {
  auto text_index_schema = std::make_shared<indexes::text::TextIndexSchema>(
      data_model::LANGUAGE_ENGLISH, ".", true, std::vector<std::string>{});

  auto predicate = std::make_unique<query::TermPredicate>(
      text_index_schema, 1ULL, "hello", false);

  EXPECT_TRUE(query::ContainsTextPredicate(predicate.get()));
}

TEST_F(ContainsTextPredicateTest, PrefixPredicateReturnsTrue) {
  auto text_index_schema = std::make_shared<indexes::text::TextIndexSchema>(
      data_model::LANGUAGE_ENGLISH, ".", true, std::vector<std::string>{});

  auto predicate = std::make_unique<query::PrefixPredicate>(
      text_index_schema, 1ULL, "hel");

  EXPECT_TRUE(query::ContainsTextPredicate(predicate.get()));
}

TEST_F(ContainsTextPredicateTest, ComposedAndWithTextReturnsTrue) {
  auto text_index_schema = std::make_shared<indexes::text::TextIndexSchema>(
      data_model::LANGUAGE_ENGLISH, ".", true, std::vector<std::string>{});

  data_model::NumericIndex numeric_index_proto;
  auto numeric_index = std::make_shared<indexes::Numeric>(numeric_index_proto);

  auto text_pred = std::make_unique<query::TermPredicate>(
      text_index_schema, 1ULL, "hello", false);
  auto numeric_pred = std::make_unique<query::NumericPredicate>(
      numeric_index.get(), "alias", "identifier", 0.0, true, 100.0, true);

  auto composed = std::make_unique<query::ComposedPredicate>(
      std::move(text_pred), std::move(numeric_pred), query::LogicalOperator::kAnd);

  EXPECT_TRUE(query::ContainsTextPredicate(composed.get()));
}

TEST_F(ContainsTextPredicateTest, ComposedOrWithTextReturnsTrue) {
  auto text_index_schema = std::make_shared<indexes::text::TextIndexSchema>(
      data_model::LANGUAGE_ENGLISH, ".", true, std::vector<std::string>{});

  data_model::TagIndex tag_index_proto;
  tag_index_proto.set_separator(",");
  auto tag_index = std::make_shared<indexes::Tag>(tag_index_proto);
  absl::flat_hash_set<absl::string_view> tags = {"tag1"};

  auto text_pred = std::make_unique<query::TermPredicate>(
      text_index_schema, 1ULL, "hello", false);
  auto tag_pred = std::make_unique<query::TagPredicate>(
      tag_index.get(), "alias", "identifier", "tag1", tags);

  auto composed = std::make_unique<query::ComposedPredicate>(
      std::move(text_pred), std::move(tag_pred), query::LogicalOperator::kOr);

  EXPECT_TRUE(query::ContainsTextPredicate(composed.get()));
}

TEST_F(ContainsTextPredicateTest, ComposedWithoutTextReturnsFalse) {
  data_model::NumericIndex numeric_index_proto;
  auto numeric_index = std::make_shared<indexes::Numeric>(numeric_index_proto);

  data_model::TagIndex tag_index_proto;
  tag_index_proto.set_separator(",");
  auto tag_index = std::make_shared<indexes::Tag>(tag_index_proto);
  absl::flat_hash_set<absl::string_view> tags = {"tag1"};

  auto numeric_pred = std::make_unique<query::NumericPredicate>(
      numeric_index.get(), "alias", "identifier", 0.0, true, 100.0, true);
  auto tag_pred = std::make_unique<query::TagPredicate>(
      tag_index.get(), "alias", "identifier", "tag1", tags);

  auto composed = std::make_unique<query::ComposedPredicate>(
      std::move(numeric_pred), std::move(tag_pred), query::LogicalOperator::kAnd);

  EXPECT_FALSE(query::ContainsTextPredicate(composed.get()));
}

TEST_F(ContainsTextPredicateTest, NegatedTextPredicateReturnsTrue) {
  auto text_index_schema = std::make_shared<indexes::text::TextIndexSchema>(
      data_model::LANGUAGE_ENGLISH, ".", true, std::vector<std::string>{});

  auto text_pred = std::make_unique<query::TermPredicate>(
      text_index_schema, 1ULL, "hello", false);
  auto negated = std::make_unique<query::NegatePredicate>(std::move(text_pred));

  EXPECT_TRUE(query::ContainsTextPredicate(negated.get()));
}

TEST_F(ContainsTextPredicateTest, NegatedNonTextPredicateReturnsFalse) {
  data_model::NumericIndex numeric_index_proto;
  auto numeric_index = std::make_shared<indexes::Numeric>(numeric_index_proto);

  auto numeric_pred = std::make_unique<query::NumericPredicate>(
      numeric_index.get(), "alias", "identifier", 0.0, true, 100.0, true);
  auto negated = std::make_unique<query::NegatePredicate>(std::move(numeric_pred));

  EXPECT_FALSE(query::ContainsTextPredicate(negated.get()));
}

// Test configuration getters for in-flight blocking
class InFlightBlockingConfigTest : public ValkeySearchTest {};

TEST_F(InFlightBlockingConfigTest, TimeoutConfigurationDefaults) {
  // Verify that the timeout configuration has sensible defaults
  auto timeout_ms = query::options::GetTextQueryInFlightWaitTimeoutMs().GetValue();
  EXPECT_GT(timeout_ms, 0);
}

}  // namespace

}  // namespace valkey_search
