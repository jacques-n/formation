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
import java.util.stream.Collectors;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.VectorRoot;
import org.apache.arrow.flight.example.InMemoryStore;
import org.apache.arrow.flight.example.Stream.StreamCreator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.WritePartition;
import com.google.common.collect.ImmutableList;

public class FormationRecordWriter implements RecordWriter {

  private String path;
  private VectorRoot root;
  private InMemoryStore store;
  private VectorUnloader unloader;
  private StreamCreator creator;

  public FormationRecordWriter(String path, InMemoryStore store) {
    super();
    this.path = path;
    this.store = store;
  }

  @Override
  public void close() throws Exception {
    if(creator != null) {
      creator.complete();
    }
  }


  @Override
  public void setup(VectorAccessible incoming, OutputEntryListener listener, WriteStatsListener statsListener) throws IOException {
    root = new VectorRoot(ImmutableList.copyOf(incoming)
        .stream()
        .map(vw -> ((FieldVector)vw.getValueVector()))
        .collect(Collectors.toList()));
    unloader = new VectorUnloader(root);
    creator = store.putStream(FlightDescriptor.path(path), root.getSchema());
  }

  @Override
  public void startPartition(WritePartition partition) throws Exception {
  }

  @Override
  public int writeBatch(int offset, int length) throws IOException {
    root.setRowCount(length);
    try(ArrowRecordBatch arb = unloader.getRecordBatch()){
      int size = arb.computeBodyLength();
      creator.add(arb);
      return size;
    }
  }

  @Override
  public void abort() throws IOException {
    creator.drop();
  }



}
