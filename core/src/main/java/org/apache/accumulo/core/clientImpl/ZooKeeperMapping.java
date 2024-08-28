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
package org.apache.accumulo.core.clientImpl;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.zookeeper.KeeperException;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class ZooKeeperMapping {
  private static final Gson gson = new Gson();

  public static void initializeNamespaceMap(ZooReaderWriter zoo, String zPath)
      throws InterruptedException, KeeperException {
    List<String> namespaceIds = zoo.getChildren(zPath);
    Map<String,String> namespaceMap = new HashMap<>();
    for (String id : namespaceIds) {
      switch (id) {
        case "+default":
          namespaceMap.put(id, Namespace.DEFAULT.name());
          break;
        case "+accumulo":
          namespaceMap.put(id, Namespace.ACCUMULO.name());
          break;
        default:
          break;
      }
    }
    String jsonNamespaces = gson.toJson(namespaceMap);
    zoo.putPersistentData(zPath, jsonNamespaces.getBytes(StandardCharsets.UTF_8),
        ZooUtil.NodeExistsPolicy.OVERWRITE);
  }

  public static void appendNamespaceToMap(ZooReaderWriter zoo, String zPath,
      NamespaceId namespaceId, String namespaceName, ZooUtil.NodeExistsPolicy existsPolicy)
      throws InterruptedException, KeeperException {
    if (!Namespace.DEFAULT.id().equals(namespaceId)
        && !Namespace.ACCUMULO.id().equals(namespaceId)) {
      byte[] data = zoo.getData(zPath);
      String jsonData = new String(data, StandardCharsets.UTF_8);
      Type type = new TypeToken<Map<String,String>>() {}.getType();
      Map<String,String> namespaceMap = gson.fromJson(jsonData, type);
      namespaceMap.put(namespaceId.toString(), namespaceName);
      String serializedJson = gson.toJson(namespaceMap);
      zoo.putPersistentData(zPath, serializedJson.getBytes(StandardCharsets.UTF_8), existsPolicy);
    }
  }

  public static Map<String,String> getNamespaceMap(ZooCache zc, String zPath)
      throws KeeperException, InterruptedException {
    byte[] data = zc.get(zPath);
    if (data == null) {
      return new HashMap<>();
    }
    String jsonData = new String(data, StandardCharsets.UTF_8);
    Type type = new TypeToken<Map<String,String>>() {}.getType();
    return gson.fromJson(jsonData, type);
  }
}
