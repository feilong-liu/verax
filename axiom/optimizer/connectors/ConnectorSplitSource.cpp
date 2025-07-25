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

#include "axiom/optimizer/connectors/ConnectorSplitSource.h"

namespace facebook::velox::connector {

std::vector<runner::SplitSource::SplitAndGroup> ConnectorSplitSource::getSplits(
    uint64_t targetBytes) {
  auto splits = source_->getSplits(targetBytes);
  std::vector<SplitAndGroup> runnerSplits;
  // convert the connector::SplitSource::SplitAndGroup to
  // runner::SplitSource::SplitAndGroup.
  for (auto& s : splits) {
    runnerSplits.push_back({s.split, s.group});
  }
  return runnerSplits;
}

std::shared_ptr<runner::SplitSource>
ConnectorSplitSourceFactory::splitSourceForScan(
    const core::TableScanNode& scan) {
  auto handle = scan.tableHandle();
  auto connector = connector::getConnector(handle->connectorId());
  auto partitions =
      connector->metadata()->splitManager()->listPartitions(handle);
  auto source = connector->metadata()->splitManager()->getSplitSource(
      handle, partitions, options_);
  return std::make_shared<ConnectorSplitSource>(std::move(source));
}

} // namespace facebook::velox::connector
