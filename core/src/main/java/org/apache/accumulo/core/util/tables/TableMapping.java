/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.util.tables;

import static java.util.Collections.emptySortedMap;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.clientImpl.NamespaceMapping.deserializeMap;
import static org.apache.accumulo.core.clientImpl.NamespaceMapping.serializeMap;

import java.util.Map;
import java.util.SortedMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSortedMap;

public class TableMapping {

  private final ClientContext context;
  private final NamespaceId namespaceId;
  private final String mappingPath;
  private volatile SortedMap<TableId,String> currentTableMap = emptySortedMap();
  private volatile SortedMap<String,TableId> currentTableReverseMap = emptySortedMap();
  private volatile long lastMzxid;

  public TableMapping(ClientContext context, NamespaceId namespaceId) {
    this.context = requireNonNull(context);
    this.namespaceId = requireNonNull(namespaceId);
    this.mappingPath = getZTableMapPath(namespaceId);
  }

  public void put(TableId tableId, String tableName, TableOperation operation)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    requireNonNull(tableId);
    requireNonNull(tableName);
    requireNonNull(operation);
    if (isBuiltInZKTable(tableId)) {
      throw new AssertionError("Putting built-in tables in map should not be possible after init");
    }
    context.getZooSession().asReaderWriter().mutateExisting(mappingPath, data -> {
      var tables = deserializeMap(data);
      final String currentName = tables.get(tableId.canonical());
      if (tableName.equals(currentName)) {
        return null; // mapping already exists; operation is idempotent, so no change needed
      }
      if (currentName != null) {
        throw new AcceptableThriftTableOperationException(null, tableId.canonical(), operation,
            TableOperationExceptionType.EXISTS, "Table Id already exists");
      }
      if (tables.containsValue(tableName)) {
        throw new AcceptableThriftTableOperationException(null, tableId.canonical(), operation,
            TableOperationExceptionType.EXISTS, "Table name already exists");
      }
      tables.put(tableId.canonical(), tableName);
      return serializeMap(tables);
    });
  }

  public void remove(final TableId tableId)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    requireNonNull(tableId);
    if (isBuiltInZKTable(tableId)) {
      throw new AssertionError("Removing built-in tables in map should not be possible");
    }
    context.getZooSession().asReaderWriter().mutateExisting(mappingPath, data -> {
      var tables = deserializeMap(data);
      if (!tables.containsKey(tableId.canonical())) {
        throw new AcceptableThriftTableOperationException(null, tableId.canonical(),
            TableOperation.DELETE, TableOperationExceptionType.NOTFOUND,
            "Table already removed while processing");
      }
      tables.remove(tableId.canonical());
      return serializeMap(tables);
    });
  }

  public void rename(final TableId tableId, final String oldName, final String newName)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    requireNonNull(tableId);
    requireNonNull(oldName);
    requireNonNull(newName);
    if (isBuiltInZKTable(tableId)) {
      throw new AssertionError("Renaming built-in tables in map should not be possible");
    }
    context.getZooSession().asReaderWriter().mutateExisting(mappingPath, data -> {
      var tables = deserializeMap(data);
      final String currentName = tables.get(tableId.canonical());
      if (newName.equals(currentName)) {
        return null; // assume in this case the operation is running again, so we are done
      }
      if (!oldName.equals(currentName)) {
        throw new AcceptableThriftTableOperationException(null, oldName, TableOperation.RENAME,
            TableOperationExceptionType.NOTFOUND, "Name changed while processing");
      }
      if (tables.containsValue(newName)) {
        throw new AcceptableThriftTableOperationException(null, newName, TableOperation.RENAME,
            TableOperationExceptionType.EXISTS, "Table name already exists");
      }
      tables.put(tableId.canonical(), newName);
      return serializeMap(tables);
    });
  }

  private synchronized void update() {
    final ZooCache zc = context.getZooCache();
    final ZcStat stat = new ZcStat();

    byte[] data = zc.get(mappingPath, stat);
    if (stat.getMzxid() > lastMzxid) {
      if (data == null) {
        throw new IllegalStateException(mappingPath + " node should not be null");
      }
      Map<String,String> idToName = deserializeMap(data);
      if (namespaceId.equals(Namespace.ACCUMULO.id())) {
        for (TableId tid : AccumuloTable.allTableIds()) {
          if (!idToName.containsKey(tid.canonical())) {
            throw new IllegalStateException("Built-in tables are not present in map");
          }
        }
      }
      var converted = ImmutableSortedMap.<TableId,String>naturalOrder();
      var convertedReverse = ImmutableSortedMap.<String,TableId>naturalOrder();
      idToName.forEach((idString, name) -> {
        var id = TableId.of(idString);
        converted.put(id, name);
        convertedReverse.put(name, id);
      });
      currentTableMap = converted.build();
      currentTableReverseMap = convertedReverse.build();

      lastMzxid = stat.getMzxid();
    } else if (data == null) { // If namespace happens to be deleted
      currentTableMap = emptySortedMap();
      currentTableReverseMap = emptySortedMap();
    }
  }

  public static String getZTableMapPath(NamespaceId namespaceId) {
    return Constants.ZNAMESPACES + "/" + namespaceId + Constants.ZTABLES;
  }

  private static boolean isBuiltInZKTable(TableId tableId) {
    return AccumuloTable.allTableIds().contains(tableId);
  }

  public SortedMap<TableId,String> getIdToNameMap() {
    update();
    return currentTableMap;
  }

  public SortedMap<String,TableId> getNameToIdMap() {
    update();
    return currentTableReverseMap;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass()).add("namespaceId", namespaceId)
        .add("currentTableMap", currentTableMap).toString();
  }

}
