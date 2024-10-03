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
package org.apache.accumulo.server.tables;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.NamespaceMapping;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class TestZKImpl {
  public static void main(String[] args) {
    try {
      // Initialize ServerContext and ZooReaderWriter
      ZooKeeper zoo = new ZooKeeper("localhost:2181/", 2000, null);
      File file = new File(
              "/home/ec2-user/Projects/fluo-uno/install/accumulo-2.1.3/conf/accumulo.properties");
      var siteConfig = SiteConfiguration.fromFile(file).build();
      ServerContext context = new ServerContext(siteConfig);
      ZooReaderWriter zooReaderWriter = new ZooReaderWriter(siteConfig);

      String zPath = Constants.ZROOT + "/" + context.getInstanceID() + Constants.ZNAMESPACES;

      // Initialize the namespace map in ZooKeeper
      NamespaceMapping.initializeNamespaceMap(zooReaderWriter, zPath);

      // Verify that the JSON data is created and stored in ZooKeeper
      byte[] initialData = zoo.getData(zPath, false, null);
      if (initialData != null) {
        System.out.println("Initial namespace map in ZooKeeper: " + new String(initialData, UTF_8));
      } else {
        System.out.println("No initial data found at path: " + zPath);
      }

      // Create and append dummy namespace
      NamespaceId dummyNamespaceId = NamespaceId.of("testNamespaceId");
      String dummyNamespaceName = "testNamespace";
      TableManager.prepareNewNamespaceState(context, dummyNamespaceId, dummyNamespaceName,
              ZooUtil.NodeExistsPolicy.OVERWRITE);

      // Retrieve the updated namespace map from ZooKeeper
      byte[] updatedData = zoo.getData(zPath, false, null);
      if (updatedData != null) {
        System.out.println("Updated namespace map in ZooKeeper: " + new String(updatedData, UTF_8));
      } else {
        System.out.println("No updated data found at path: " + zPath);
      }

      // Retrieve map from ZooCache: Run #1, initial
      Map<NamespaceId,String> namespaceMap1 = context.getNamespaces().getIdToNameMap();
      Map<String,NamespaceId> namespaceMap2 = context.getNamespaces().getNameToIdMap();
      System.out.println("\nRetrieve Id to Name map from ZC #1: " + namespaceMap1);
      System.out.println("Retrieve Name to Id map from ZC #1: " + namespaceMap2);
      System.out.println("Retrieve Id to Name map from ZK #1: " + new String(zoo.getData(zPath, false, null), UTF_8));

      // Retrieve map from ZooCache: Run #2, with no change
      Map<NamespaceId,String> namespaceMap3 = context.getNamespaces().getIdToNameMap();
      Map<String,NamespaceId> namespaceMap4 = context.getNamespaces().getNameToIdMap();
      System.out.println("\nRetrieve Id to Name map from ZC #2: " + namespaceMap3);
      System.out.println("Retrieve Name to Id map from ZC #2: " + namespaceMap4);
      System.out.println("Retrieve Id to Name map from ZK #2: " + new String(zoo.getData(zPath, false, null), UTF_8));

      // Create and append dummy namespace 2 for ZC retrieval #3
      NamespaceId dummyNamespaceId2 = NamespaceId.of("testNamespaceId2");
      String dummyNamespaceName2 = "testNamespace2";
      TableManager.prepareNewNamespaceState(context, dummyNamespaceId2, dummyNamespaceName2,
              ZooUtil.NodeExistsPolicy.OVERWRITE);

      // Retrieve map from ZooCache: Run #3, with new change
      Map<NamespaceId,String> namespaceMap5 = context.getNamespaces().getIdToNameMap();
      Map<String,NamespaceId> namespaceMap6 = context.getNamespaces().getNameToIdMap();
      System.out.println("\nRetrieve Id to Name map from ZC #3: " + namespaceMap5);
      System.out.println("Retrieve Id to Name map from ZC #3: " + namespaceMap6);
      System.out.println("Retrieve Id to Name map from ZK #3: " + new String(zoo.getData(zPath, false, null), UTF_8));

      // Create and append dummy namespace 3 for ZC retrieval #4
      NamespaceId dummyNamespaceId3 = NamespaceId.of("testNamespaceId3");
      String dummyNamespaceName3 = "testNamespace3";
      TableManager.prepareNewNamespaceState(context, dummyNamespaceId3, dummyNamespaceName3,
              ZooUtil.NodeExistsPolicy.OVERWRITE);

      // Retrieve map from ZooCache: Run #4, with new change 2
      Map<NamespaceId,String> namespaceMap7 = context.getNamespaces().getIdToNameMap();
      Map<String,NamespaceId> namespaceMap8 = context.getNamespaces().getNameToIdMap();
      System.out.println("\nRetrieve Id to Name map from ZC #4: " + namespaceMap7);
      System.out.println("Retrieve Id to Name map from ZC #4: " + namespaceMap8);
      System.out.println("Retrieve Id to Name map from ZK #4: " + new String(zoo.getData(zPath, false, null), UTF_8));

      // Test getNamespaceId
      System.out.println("\ngetNamespaceId(test1): " + Namespaces.getNamespaceId(context, "testNamespace"));
      System.out.println("getNamespaceId(test2): " + Namespaces.getNamespaceId(context, "testNamespace2"));

      // Test getNamespaceName
      System.out.println("\ngetNamespaceName(test1): " + Namespaces.getNamespaceName(context, NamespaceId.of("testNamespaceId")));
      System.out.println("getNamespaceName(test2): " + Namespaces.getNamespaceName(context, NamespaceId.of("testNamespaceId2")));
    } catch (KeeperException | InterruptedException | IOException | IllegalArgumentException |
             NamespaceNotFoundException e) {
      e.printStackTrace();
    }
  }
}
