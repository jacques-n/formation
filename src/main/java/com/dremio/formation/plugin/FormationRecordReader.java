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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.VectorRoot;
import org.apache.arrow.flight.example.InMemoryStore;
import org.apache.arrow.flight.example.Stream;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;

public class FormationRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormationRecordReader.class);

  private final InMemoryStore store;
  private final Ticket ticket;

  private VectorRoot root;
  private Stream stream;
  private VectorLoader loader;
  private List<TransferPair> transfers = new ArrayList<>();
  private Iterator<ArrowRecordBatch> batches;


  public FormationRecordReader(
      final OperatorContext context,
      List<SchemaPath> columns,
      InMemoryStore store,
      Ticket ticket) {
    super(context, columns);
    this.ticket = ticket;
    this.store = store;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    stream = store.getStream(ticket);
    root = VectorRoot.create(stream.getSchema(), context.getAllocator());
    loader = new VectorLoader(root);
    batches = stream.iterator();

    Map<String, ValueVector> inputVectorMap = new HashMap<>();
    for(ValueVector v : root.getFieldVectors()) {
      inputVectorMap.put(v.getField().getName(), v);
    }

    for(SchemaPath p : getColumns()) {
      ValueVector inputVector = inputVectorMap.get(p.getRootSegment().getPath());
      ValueVector outputVector = output.addField(inputVector.getField(), ValueVector.class);
      transfers.add(inputVector.makeTransferPair(outputVector));
    }
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    // no-op as this allocates buffers based on the size of the buffers in file.
  }

  @Override
  public int next() {
    if(!batches.hasNext()) {
      return 0;
    }
    ArrowRecordBatch arb = batches.next();
    loader.load(arb);
    for(TransferPair tp : transfers) {
      tp.transfer();
    }
    return arb.getLength();
  }

  @Override
  protected boolean supportsSkipAllQuery() {
    return true;
  }

  @Override
  public void close() throws Exception {
    root.close();
  }
}
