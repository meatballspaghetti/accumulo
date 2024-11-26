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
package org.apache.accumulo.server.util;

import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class ZooZap implements KeywordExecutable {
  private static final Logger log = LoggerFactory.getLogger(ZooZap.class);

  private static void message(String msg, Opts opts) {
    if (opts.verbose) {
      System.out.println(msg);
    }
  }

  @Override
  public String keyword() {
    return "zoo-zap";
  }

  @Override
  public String description() {
    return "Utility for zapping Zookeeper locks";
  }

  static class Opts extends Help {
    @Parameter(names = "-manager", description = "remove manager locks")
    boolean zapManager = false;
    @Parameter(names = "-tservers", description = "remove tablet server locks")
    boolean zapTservers = false;
    @Parameter(names = "-compaction-coordinators",
        description = "remove compaction coordinator locks")
    boolean zapCoordinators = false;
    @Parameter(names = "-compactors", description = "remove compactor locks")
    boolean zapCompactors = false;
    @Parameter(names = "-sservers", description = "remove scan server locks")
    boolean zapScanServers = false;
    @Parameter(names = "-verbose", description = "print out messages about progress")
    boolean verbose = false;
  }

  public static void main(String[] args) throws Exception {
    new ZooZap().execute(args);
  }

  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(keyword(), args);

    if (!opts.zapManager && !opts.zapTservers) {
      new JCommander(opts).usage();
      return;
    }

    var siteConf = SiteConfiguration.auto();
    try (var zk = new ZooSession(getClass().getSimpleName(), siteConf)) {
      // Login as the server on secure HDFS
      if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        SecurityUtil.serverLogin(siteConf);
      }

      String volDir = VolumeConfiguration.getVolumeUris(siteConf).iterator().next();
      Path instanceDir = new Path(volDir, "instance_id");
      InstanceId iid = VolumeManager.getInstanceIDFromHdfs(instanceDir, new Configuration());
      var zrw = zk.asReaderWriter();

      if (opts.zapManager) {
        try {
          zapDirectory(zrw, Constants.ZMANAGER_LOCK, opts);
        } catch (KeeperException | InterruptedException e) {
          e.printStackTrace();
        }
      }

      if (opts.zapTservers) {
        try {
          List<String> children = zrw.getChildren(Constants.ZTSERVERS);
          for (String child : children) {
            message("Deleting " + Constants.ZTSERVERS + "/" + child + " from zookeeper", opts);

            if (opts.zapManager) {
              zrw.recursiveDelete(Constants.ZTSERVERS + "/" + child, NodeMissingPolicy.SKIP);
            } else {
              var zLockPath = ServiceLock.path(Constants.ZTSERVERS + "/" + child);
              if (!zrw.getChildren(zLockPath.toString()).isEmpty()) {
                if (!ServiceLock.deleteLock(zrw, zLockPath, "tserver")) {
                  message("Did not delete " + Constants.ZTSERVERS + "/" + child, opts);
                }
              }
            }
          }
        } catch (KeeperException | InterruptedException e) {
          log.error("{}", e.getMessage(), e);
        }
      }

      if (opts.zapCoordinators) {
        try {
          if (zrw.exists(Constants.ZCOORDINATOR_LOCK)) {
            zapDirectory(zrw, Constants.ZCOORDINATOR_LOCK, opts);
          }
        } catch (KeeperException | InterruptedException e) {
          log.error("Error deleting coordinator from zookeeper, {}", e.getMessage(), e);
        }
      }

      if (opts.zapCompactors) {
        try {
          if (zrw.exists(Constants.ZCOMPACTORS)) {
            List<String> queues = zrw.getChildren(Constants.ZCOMPACTORS);
            for (String queue : queues) {
              message("Deleting " + Constants.ZCOMPACTORS + "/" + queue + " from zookeeper", opts);
              zrw.recursiveDelete(Constants.ZCOMPACTORS + "/" + queue, NodeMissingPolicy.SKIP);
            }
          }
        } catch (KeeperException | InterruptedException e) {
          log.error("Error deleting compactors from zookeeper, {}", e.getMessage(), e);
        }

      }

      if (opts.zapScanServers) {
        try {
          if (zrw.exists(Constants.ZSSERVERS)) {
            List<String> children = zrw.getChildren(Constants.ZSSERVERS);
            for (String child : children) {
              message("Deleting " + Constants.ZSSERVERS + "/" + child + " from zookeeper", opts);

              var zLockPath = ServiceLock.path(Constants.ZSSERVERS + "/" + child);
              if (!zrw.getChildren(zLockPath.toString()).isEmpty()) {
                ServiceLock.deleteLock(zrw, zLockPath);
              }
            }
          }
        } catch (KeeperException | InterruptedException e) {
          log.error("{}", e.getMessage(), e);
        }
      }

    } finally {
      SingletonManager.setMode(Mode.CLOSED);
    }

  }

  private static void zapDirectory(ZooReaderWriter zoo, String path, Opts opts)
      throws KeeperException, InterruptedException {
    List<String> children = zoo.getChildren(path);
    for (String child : children) {
      message("Deleting " + path + "/" + child + " from zookeeper", opts);
      zoo.recursiveDelete(path + "/" + child, NodeMissingPolicy.SKIP);
    }
  }
}
