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

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.google.common.base.Preconditions;

/**
 * Physical scan operator.
 */
public class FormationScanPrel extends ScanPrelBase {

  public FormationScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      TableMetadata dataset,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment
      ) {
    super(cluster, traitSet, table, dataset.getStoragePluginId(), dataset, projectedColumns, observedRowcountAdjustment);
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.HARD;
  }


  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return creator.addMetadata(this, new FormationGroupScan(getTableMetadata(), getProjectedColumns()));
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    Preconditions.checkArgument(inputs == null || inputs.size() == 0);
    return new FormationScanPrel(getCluster(), traitSet, getTable(), getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor());
  }

  @Override
  public FormationScanPrel cloneWithProject(List<SchemaPath> projection) {
    return new FormationScanPrel(getCluster(), getTraitSet(), getTable(), getTableMetadata(), projection, getCostAdjustmentFactor());
  }

}
