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
import java.util.stream.Collectors;

import org.apache.arrow.flight.Ticket;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

public class FormationScanCreator implements ProducerOperator.Creator<FormationSubScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec, OperatorContext context, FormationSubScan config) throws ExecutionSetupException {
    final FormationPlugin plugin = fec.getStoragePlugin(config.getPluginId());
    List<RecordReader> readers = config.getSplits()
        .stream()
        .map(s -> new FormationRecordReader(
            context,
            config.getColumns(),
            plugin.getStore(),
            new Ticket(s.getExtendedProperty().toByteArray())))
        .collect(Collectors.toList());
    return new ScanOperator(fec.getSchemaUpdater(), config, context, readers.iterator());
  }
}
