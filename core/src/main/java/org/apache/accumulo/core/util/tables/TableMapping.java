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
import java.util.Objects;
import java.util.SortedMap;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableSortedMap;

public class TableMapping {

  private final ClientContext context;
  private final NamespaceId namespaceId;
  private volatile SortedMap<TableId,String> currentTableMap = emptySortedMap();
  private volatile SortedMap<String,TableId> currentTableReverseMap = emptySortedMap();
  private volatile long lastMzxid;

  public TableMapping(ClientContext context, NamespaceId namespaceId) {
    this.context = context;
    this.namespaceId = namespaceId;
  }

  public static void put(final ZooReaderWriter zoo, TableId tableId, NamespaceId namespaceId,
      String tableName, TableOperation operation)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    Stream.of(zoo, tableId, namespaceId, tableName).forEach(Objects::requireNonNull);
    String zTableMapPath = getZTableMapPath(namespaceId);
    if (!zoo.exists(zTableMapPath)) {
      throw new KeeperException.NoNodeException(zTableMapPath + " does not exist in ZooKeeper");
    }
    if (isBuiltInZKTable(tableId)) {
      throw new AssertionError("Putting built-in tables in map should not be possible after init");
    }
    zoo.mutateExisting(zTableMapPath, data -> {
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

  public static void remove(final ZooReaderWriter zoo, final TableId tableId)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    Stream.of(zoo, tableId).forEach(Objects::requireNonNull);
    if (isBuiltInZKTable(tableId)) {
      throw new AssertionError("Removing built-in tables in map should not be possible");
    }
    zoo.mutateExisting(getZTableMapPath(getNamespaceOfTableId(zoo, tableId)), data -> {
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

  public static void rename(final ZooReaderWriter zoo, final TableId tableId,
      final NamespaceId namespaceId, final String oldName, final String newName)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    Stream.of(zoo, tableId, namespaceId, oldName, newName).forEach(Objects::requireNonNull);
    String zTableMapPath = getZTableMapPath(namespaceId);
    if (!zoo.exists(zTableMapPath)) {
      throw new KeeperException.NoNodeException(zTableMapPath + " does not exist in ZooKeeper");
    }
    if (isBuiltInZKTable(tableId)) {
      throw new AssertionError("Renaming built-in tables in map should not be possible");
    }
    zoo.mutateExisting(zTableMapPath, current -> {
      var tables = deserializeMap(current);
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
      tables.put(namespaceId.canonical(), newName);
      return serializeMap(tables);
    });
  }

  private synchronized void update(NamespaceId namespaceId) {
    final ZooCache zc = context.getZooCache();
    final ZcStat stat = new ZcStat();
    final String zTableMapPath = getZTableMapPath(namespaceId);

    byte[] data = zc.get(zTableMapPath, stat);
    if (stat.getMzxid() > lastMzxid) {
      if (data == null) {
        throw new IllegalStateException(zTableMapPath + " node should not be null");
      } else {
        Map<String,String> idToName = deserializeMap(data);
        if (namespaceId == Namespace.ACCUMULO.id() && AccumuloTable.builtInTableIds().stream()
            .anyMatch(bitid -> !idToName.containsKey(bitid.canonical()))) {
          throw new IllegalStateException("Built-in tables are not present in map");
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
      }
      lastMzxid = stat.getMzxid();
    }
  }

  public static String getZTableMapPath(NamespaceId namespaceId) {
    return Constants.ZNAMESPACES + "/" + namespaceId + Constants.ZTABLES;
  }

  private static boolean isBuiltInZKTable(TableId tableId) {
    return AccumuloTable.builtInTableIds().contains(tableId);
  }

  private static NamespaceId getNamespaceOfTableId(ZooReaderWriter zoo, TableId tableId)
      throws IllegalArgumentException, InterruptedException, KeeperException {
    for (String nid : zoo.getChildren(Constants.ZNAMESPACES)) {
      for (String tid : zoo.getChildren(Constants.ZNAMESPACES + "/" + nid)) {
        if (TableId.of(tid).equals(tableId)) {
          return NamespaceId.of(nid);
        }
      }
    }
    throw new KeeperException.NoNodeException(
        "TableId " + tableId.canonical() + " does not exist in ZooKeeper");
  }

  public SortedMap<TableId,String> getIdToNameMap(NamespaceId namespaceId) {
    update(namespaceId);
    return currentTableMap;
  }

  public SortedMap<String,TableId> getNameToIdMap(NamespaceId namespaceId) {
    update(namespaceId);
    return currentTableReverseMap;
  }

}
