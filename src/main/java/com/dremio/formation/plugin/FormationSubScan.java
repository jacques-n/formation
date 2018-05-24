/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.formation.plugin;

import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Easy sub scan.
 */
@JsonTypeName("formation-sub-scan")
public class FormationSubScan extends SubScanWithProjection {
  private final List<DatasetSplit> splits;
  private final StoragePluginId pluginId;

  @JsonCreator
  public FormationSubScan(
    @JsonProperty("splits") List<DatasetSplit> splits,
    @JsonProperty("userName") String userName,
    @JsonProperty("schema") BatchSchema schema,
    @JsonProperty("tableSchemaPath") List<String> tablePath,
    @JsonProperty("pluginId") StoragePluginId pluginId,
    @JsonProperty("columns") List<SchemaPath> columns) {
    super(userName, schema, tablePath, columns);
    this.splits = splits;
    this.pluginId = pluginId;
  }

  public List<DatasetSplit> getSplits() {
    return splits;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.ARROW_SUB_SCAN_VALUE;
  }
}
