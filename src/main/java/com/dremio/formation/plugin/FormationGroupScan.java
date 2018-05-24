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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.dataset.proto.Affinity;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;

/**
 * System group scan.
 */
public class FormationGroupScan extends AbstractGroupScan {

  public FormationGroupScan(
      TableMetadata dataset,
      List<SchemaPath> columns
      ) {
    super(dataset, columns);
  }

  // If distributed, the scan needs to happen on every node.
  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    return dataset.getSplitCount();
  }

  @Override
  public final int getMinParallelizationWidth() {
    final Set<String> nodes = new HashSet<>();
    Iterator<DatasetSplit> iter = dataset.getSplits();
    while(iter.hasNext()){
      DatasetSplit split = iter.next();
      for(Affinity a : split.getAffinitiesList()){
        nodes.add(a.getHost());
      }
    }

    return nodes.size();
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.HARD;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.ARROW_SUB_SCAN.ordinal();
  }

  @JsonIgnore
  @Override
  public List<List<String>> getReferencedTables() {
    return ImmutableList.of();
  }

  @Override
  public boolean mayLearnSchema() {
    return false;
  }

  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitGroupScan(this, value);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext context) {
    return getSchema().maskAndReorder(getColumns());
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    return new FormationSubScan(
        work.stream().map(t -> t.getSplit()).collect(Collectors.toList()),
        getUserName(),
        getSchema(),
        getTableSchemaPath(),
        dataset.getStoragePluginId(),
        getColumns());
  }


}
