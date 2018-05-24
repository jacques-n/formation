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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.example.ExampleFlightServer;
import org.apache.arrow.flight.example.InMemoryStore;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.CanCreateTable;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.dfs.GenericCreateTableEntry;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

public class FormationPlugin implements CanCreateTable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormationPlugin.class);

  private static final int FLIGHT_PORT = 12233;
  private final BufferAllocator allocator;
  private final SabotContext context;
  private final ExampleFlightServer server;
  private volatile List<FlightClient> clients = new ArrayList<>();
  private final Provider<StoragePluginId> pluginIdProvider;

  public FormationPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    this.context = context;
    this.allocator = context.getAllocator().newChildAllocator("formation-" + name, 0, Long.MAX_VALUE);
    this.server = new ExampleFlightServer(this.allocator, new Location(context.getEndpoint().getAddress(), FLIGHT_PORT));
    this.pluginIdProvider = pluginIdProvider;
  }

  @Override
  public Iterable<SourceTableDefinition> getDatasets(String user, boolean ignoreAuthErrors) throws Exception {
    if(clients.isEmpty()) {
      return ImmutableList.of();
    }

    // for simplicity, grab a particular client, assuming that all data is on all nodes.
    FlightClient c = clients.get(0);
    List<NamespaceKey> keys = ImmutableList.copyOf(c.listFlights(new Criteria())).stream().map(fi -> new NamespaceKey(fi.getDescriptor().getPath())).collect(Collectors.toList());
    return keys.stream().map(k -> new DatasetBuilder(clients, k)).collect(Collectors.toList());
  }

  InMemoryStore getStore() {
    return server.getStore();
  }

  @Override
  public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldDataset, boolean ignoreAuthErrors) throws Exception {
    if(datasetPath.size() != 2) {
      return null;
    }
    try {
    return new DatasetBuilder(clients, datasetPath);
    } catch (Exception ex) {
      return null;
    }
  }

  @Override
  public boolean containerExists(NamespaceKey key) {
    return true;
  }

  @Override
  public boolean datasetExists(NamespaceKey key) {
    return true;
  }
  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }
  @Override
  public SourceState getState() {
    return SourceState.GOOD;
  }
  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }
  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return FormationRulesFactory.class;
  }
  @Override
  public CheckResult checkReadSignature(ByteString key, DatasetConfig datasetConfig) throws Exception {
    return CheckResult.UNCHANGED;
  }

  @Override
  public void start() throws IOException {
    server.start();
    context.getClusterCoordinator().getServiceSet(Role.EXECUTOR).addNodeStatusListener(new NodeStatusListener() {

      @Override
      public void nodesUnregistered(Set<NodeEndpoint> arg0) {
        refreshClients();
      }

      @Override
      public void nodesRegistered(Set<NodeEndpoint> arg0) {
        refreshClients();
      }
    });
    refreshClients();
  }

  private synchronized void refreshClients() {
    List<FlightClient> oldClients = clients;
    clients = context.getExecutors().stream()
        .map(e -> new FlightClient(allocator, new Location(e.getAddress(), FLIGHT_PORT))).collect(Collectors.toList());
    try {
    AutoCloseables.close(oldClients);
    } catch (Exception ex) {
      logger.error("Failure while refreshing clients.", ex);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(clients, ImmutableList.of(server, allocator));
  }

  @Override
  public CreateTableEntry createNewTable(NamespaceKey key, WriterOptions writerOptions, Map<String, Object> storageOptions) {
    if(key.size() != 2) {
      throw UserException.unsupportedError().message("Formation plugin currently only supports single part names.").build(logger);
    }
    return new GenericCreateTableEntry(SystemUser.SYSTEM_USERNAME, this, key.getLeaf(), writerOptions);
  }

  @Override
  public StoragePluginId getId() {
    return pluginIdProvider.get();
  }

  @Override
  public Writer getWriter(PhysicalOperator child, String userName, String location, WriterOptions options) throws IOException {
    return new FormationWriter(child, userName, location, options, this);
  }


}
