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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.Affinity;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

public class DatasetBuilder implements SourceTableDefinition {

  private final List<FlightClient> clients;
  private NamespaceKey key;

  private DatasetConfig config;
  private List<DatasetSplit> splits;
  private List<FlightInfo> infos;

  public DatasetBuilder(List<FlightClient> clients, NamespaceKey key) {
    super();
    this.clients = clients;
    this.key = key;
    buildIfNecessary();
  }

  public DatasetBuilder(List<FlightClient> clients, NamespaceKey key, List<FlightInfo> infos) {
    super();
    this.clients = clients;
    this.key = key;
    this.infos = infos;
  }

  private void buildIfNecessary() {
    if(config != null) {
      return;
    }

    if(infos == null) {
      infos = clients.stream()
          .map(c -> c.getInfo(FlightDescriptor.path(key.getLeaf())))
          .collect(Collectors.toList());
    }

    Preconditions.checkArgument(!infos.isEmpty());
    Schema schema = null;
    long records = 0;
    List<FlightEndpoint> endpoints = new ArrayList<>();
    for(FlightInfo info : infos) {
      schema = info.getSchema();
      records += info.getRecords();
      endpoints.addAll(info.getEndpoints());
    }

     config = new DatasetConfig()
         .setFullPathList(key.getPathComponents())
         .setName(key.getName())
         .setType(DatasetType.PHYSICAL_DATASET)
         .setId(new EntityId().setId(UUID.randomUUID().toString()))
         .setReadDefinition(new ReadDefinition()
             .setScanStats(new ScanStats().setRecordCount(records)
             .setScanFactor(ScanCostFactor.ARROW_MEMORY.getFactor())))
         .setOwner(SystemUser.SYSTEM_USERNAME)
         .setPhysicalDataset(new PhysicalDataset())
         .setRecordSchema(new BatchSchema(schema.getFields()).toByteString())
         .setSchemaVersion(DatasetHelper.CURRENT_VERSION);

     splits = new ArrayList<>();
     int i =0;
     for(FlightEndpoint ep : endpoints) {
       DatasetSplit split = new DatasetSplit();
       Affinity a = new Affinity();
       a.setFactor(100d);
       a.setHost(ep.getLocation().getHost());
       split.setAffinitiesList(ImmutableList.of(a));
       split.setExtendedProperty(ByteString.copyFrom(ep.getTicket().getBytes()));
       split.setSize(records/endpoints.size());
       split.setSplitKey(Integer.toString(i));
       splits.add(split);
     }
  }

  @Override
  public DatasetConfig getDataset() throws Exception {
    buildIfNecessary();
    return config;
  }

  @Override
  public NamespaceKey getName() {
    return key;
  }

  @Override
  public List<DatasetSplit> getSplits() throws Exception {
    buildIfNecessary();
    return splits;
  }

  @Override
  public DatasetType getType() {
    return DatasetType.PHYSICAL_DATASET;
  }

  @Override
  public boolean isSaveable() {
    return true;
  }

}
