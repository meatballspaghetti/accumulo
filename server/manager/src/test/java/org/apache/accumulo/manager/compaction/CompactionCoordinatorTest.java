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
package org.apache.accumulo.manager.compaction;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.USER_COMPACTION_REQUESTED;
import static org.apache.accumulo.manager.compaction.coordinator.CompactionCoordinator.canReserveCompaction;
import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.compaction.coordinator.CompactionCoordinator;
import org.apache.accumulo.manager.compaction.queue.CompactionJobPriorityQueue;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues.MetaJob;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class CompactionCoordinatorTest {

  // Need a non-null fateInstances reference for CompactionCoordinator.compactionCompleted
  private static final AtomicReference<Map<FateInstanceType,Fate<Manager>>> fateInstances =
      new AtomicReference<>(Map.of());

  private static final CompactorGroupId GROUP_ID = CompactorGroupId.of("R2DQ");

  public class TestCoordinator extends CompactionCoordinator {

    private final List<RunningCompaction> runningCompactions;

    private Set<ExternalCompactionId> metadataCompactionIds = null;

    public TestCoordinator(ServerContext ctx, SecurityOperation security,
        List<RunningCompaction> runningCompactions) {
      super(ctx, security, fateInstances);
      this.runningCompactions = runningCompactions;
    }

    @Override
    protected void startDeadCompactionDetector() {}

    @Override
    protected long getTServerCheckInterval() {
      return 5000L;
    }

    @Override
    protected void startCompactionCleaner(ScheduledThreadPoolExecutor schedExecutor) {}

    @Override
    protected void startRunningCleaner(ScheduledThreadPoolExecutor schedExecutor) {}

    @Override
    protected void startIdleCompactionWatcher() {
      // This is called from CompactionCoordinator.run(). Counting down
      // the latch will exit the run method
      this.shutdown.countDown();
    }

    @Override
    public void compactionCompleted(TInfo tinfo, TCredentials credentials,
        String externalCompactionId, TKeyExtent textent, TCompactionStats stats)
        throws ThriftSecurityException {}

    @Override
    public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
        TKeyExtent extent) throws ThriftSecurityException {}

    void setMetadataCompactionIds(Set<ExternalCompactionId> mci) {
      metadataCompactionIds = mci;
    }

    @Override
    protected Set<ExternalCompactionId> readExternalCompactionIds() {
      if (metadataCompactionIds == null) {
        return RUNNING_CACHE.keySet();
      } else {
        return metadataCompactionIds;
      }
    }

    public Map<ExternalCompactionId,RunningCompaction> getRunning() {
      return RUNNING_CACHE;
    }

    public void resetInternals() {
      getRunning().clear();
      metadataCompactionIds = null;
    }

    @Override
    protected List<RunningCompaction> getCompactionsRunningOnCompactors() {
      return runningCompactions;
    }

    @Override
    protected CompactionMetadata reserveCompaction(MetaJob metaJob, String compactorAddress,
        ExternalCompactionId externalCompactionId) {
      return createExternalCompactionMetadata(metaJob.getJob(),
          metaJob.getJob().getFiles().stream().map(CompactableFileImpl::toStoredTabletFile)
              .collect(Collectors.toSet()),
          metaJob.getTabletMetadata(), compactorAddress, externalCompactionId);
    }

    @Override
    protected CompactionMetadata createExternalCompactionMetadata(CompactionJob job,
        Set<StoredTabletFile> jobFiles, TabletMetadata tablet, String compactorAddress,
        ExternalCompactionId externalCompactionId) {
      FateInstanceType type = FateInstanceType.fromTableId(tablet.getExtent().tableId());
      FateId fateId = FateId.from(type, UUID.randomUUID());
      return new CompactionMetadata(jobFiles,
          new ReferencedTabletFile(new Path("file:///accumulo/tables/1/default_tablet/F00001.rf")),
          compactorAddress, job.getKind(), job.getPriority(), job.getGroup(), true, fateId);
    }

    @Override
    protected TExternalCompactionJob createThriftJob(String externalCompactionId,
        CompactionMetadata ecm, MetaJob metaJob, Optional<CompactionConfig> compactionConfig) {
      return new TExternalCompactionJob(externalCompactionId,
          metaJob.getTabletMetadata().getExtent().toThrift(), List.of(),
          SystemIteratorUtil.toIteratorConfig(List.of()),
          ecm.getCompactTmpName().getNormalizedPathStr(), ecm.getPropagateDeletes(),
          TCompactionKind.valueOf(ecm.getKind().name()),
          FateId
              .from(FateInstanceType.fromTableId(metaJob.getTabletMetadata().getExtent().tableId()),
                  UUID.randomUUID())
              .toThrift(),
          Map.of());
    }

    @Override
    protected void cancelCompactionOnCompactor(String address, String externalCompactionId) {}

  }

  @Test
  public void testCoordinatorColdStart() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);

    EasyMock.replay(context, security);

    var coordinator = new TestCoordinator(context, security, new ArrayList<>());
    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    coordinator.shutdown();

    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());
    EasyMock.verify(context, security);
  }

  @Test
  public void testCoordinatorRestartOneRunningCompaction() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    HostAndPort tserverAddress = HostAndPort.fromString("localhost:9997");

    List<RunningCompaction> runningCompactions = new ArrayList<>();
    ExternalCompactionId eci = ExternalCompactionId.generate(UUID.randomUUID());
    TExternalCompactionJob job = EasyMock.createNiceMock(TExternalCompactionJob.class);
    expect(job.getExternalCompactionId()).andReturn(eci.toString()).anyTimes();
    TKeyExtent extent = new TKeyExtent();
    extent.setTable("1".getBytes());
    runningCompactions
        .add(new RunningCompaction(job, tserverAddress.toString(), GROUP_ID.toString()));

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);

    EasyMock.replay(context, job, security);

    var coordinator = new TestCoordinator(context, security, runningCompactions);
    coordinator.resetInternals();
    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    coordinator.shutdown();
    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(1, coordinator.getRunning().size());

    Map<ExternalCompactionId,RunningCompaction> running = coordinator.getRunning();
    Entry<ExternalCompactionId,RunningCompaction> ecomp = running.entrySet().iterator().next();
    assertEquals(eci, ecomp.getKey());
    RunningCompaction rc = ecomp.getValue();
    assertEquals(GROUP_ID.toString(), rc.getGroupName());
    assertEquals(tserverAddress.toString(), rc.getCompactorAddress());

    EasyMock.verify(context, job, security);
  }

  @Test
  public void testGetCompactionJob() throws Exception {

    TableConfiguration tconf = EasyMock.createNiceMock(TableConfiguration.class);
    expect(tconf.get(Property.TABLE_COMPACTION_CONFIGURER))
        .andReturn(Property.TABLE_COMPACTION_CONFIGURER.getDefaultValue()).anyTimes();

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
    expect(context.getTableConfiguration(TableId.of("2a"))).andReturn(tconf).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);
    expect(context.rpcCreds()).andReturn(creds).anyTimes();

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);
    expect(security.canPerformSystemActions(creds)).andReturn(true).anyTimes();

    KeyExtent ke = new KeyExtent(TableId.of("2a"), new Text("z"), new Text("b"));
    TabletMetadata tm = EasyMock.createNiceMock(TabletMetadata.class);
    expect(tm.getExtent()).andReturn(ke).anyTimes();
    expect(tm.getFiles()).andReturn(Collections.emptySet()).anyTimes();
    expect(tm.getTableId()).andReturn(ke.tableId()).anyTimes();

    EasyMock.replay(tconf, context, creds, tm, security);

    var coordinator = new TestCoordinator(context, security, new ArrayList<>());
    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());
    // Use coordinator.run() to populate the internal data structures. This is tested in a different
    // test.
    coordinator.run();
    coordinator.shutdown();

    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());

    // Add a job to the job queue
    CompactionJob job = new CompactionJobImpl((short) 1, GROUP_ID, Collections.emptyList(),
        CompactionKind.SYSTEM, Optional.of(true));
    coordinator.addJobs(tm, Collections.singleton(job));
    CompactionJobPriorityQueue queue = coordinator.getJobQueues().getQueue(GROUP_ID);
    assertEquals(1, queue.getQueuedJobs());

    // Get the next job
    ExternalCompactionId eci = ExternalCompactionId.generate(UUID.randomUUID());
    TExternalCompactionJob createdJob = coordinator.getCompactionJob(new TInfo(), creds,
        GROUP_ID.toString(), "localhost:10241", eci.toString());
    assertEquals(eci.toString(), createdJob.getExternalCompactionId());
    assertEquals(ke, KeyExtent.fromThrift(createdJob.getExtent()));

    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(1, coordinator.getRunning().size());
    Entry<ExternalCompactionId,RunningCompaction> entry =
        coordinator.getRunning().entrySet().iterator().next();
    assertEquals(eci.toString(), entry.getKey().toString());
    assertEquals("localhost:10241", entry.getValue().getCompactorAddress());
    assertEquals(eci.toString(), entry.getValue().getJob().getExternalCompactionId());

    EasyMock.verify(tconf, context, creds, tm, security);
  }

  @Test
  public void testGetCompactionJobNoJobs() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);
    expect(security.canPerformSystemActions(creds)).andReturn(true);

    EasyMock.replay(context, creds, security);

    var coordinator = new TestCoordinator(context, security, new ArrayList<>());
    TExternalCompactionJob job = coordinator.getCompactionJob(TraceUtil.traceInfo(), creds,
        GROUP_ID.toString(), "localhost:10240", UUID.randomUUID().toString());
    assertNull(job.getExternalCompactionId());

    EasyMock.verify(context, creds, security);
  }

  @Test
  public void testCleanUpRunning() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);

    EasyMock.replay(context, creds, security);

    TestCoordinator coordinator = new TestCoordinator(context, security, new ArrayList<>());

    var ecid1 = ExternalCompactionId.generate(UUID.randomUUID());
    var ecid2 = ExternalCompactionId.generate(UUID.randomUUID());
    var ecid3 = ExternalCompactionId.generate(UUID.randomUUID());

    coordinator.getRunning().put(ecid1, new RunningCompaction(new TExternalCompaction()));
    coordinator.getRunning().put(ecid2, new RunningCompaction(new TExternalCompaction()));
    coordinator.getRunning().put(ecid3, new RunningCompaction(new TExternalCompaction()));

    coordinator.cleanUpRunning();

    assertEquals(Set.of(ecid1, ecid2, ecid3), coordinator.getRunning().keySet());

    coordinator.setMetadataCompactionIds(Set.of(ecid1, ecid2));

    coordinator.cleanUpRunning();

    assertEquals(Set.of(ecid1, ecid2), coordinator.getRunning().keySet());

    EasyMock.verify(context, creds, security);

  }

  @Test
  public void testCanReserve() throws Exception {
    TableId tableId1 = TableId.of("5");
    TableId tableId2 = TableId.of("6");

    var file1 = StoredTabletFile.of(new URI("file:///accumulo/tables/1/default_tablet/F00001.rf"));
    var file2 = StoredTabletFile.of(new URI("file:///accumulo/tables/1/default_tablet/F00002.rf"));
    var file3 = StoredTabletFile.of(new URI("file:///accumulo/tables/1/default_tablet/F00003.rf"));
    var file4 = StoredTabletFile.of(new URI("file:///accumulo/tables/1/default_tablet/F00004.rf"));

    ServerContext context = EasyMock.mock(ServerContext.class);
    EasyMock.expect(context.getTableState(tableId1)).andReturn(TableState.ONLINE).atLeastOnce();
    EasyMock.expect(context.getTableState(tableId2)).andReturn(TableState.OFFLINE).atLeastOnce();

    FateId fateId1 = FateId.from(FateInstanceType.USER, UUID.randomUUID());

    CompactorGroupId cgid = CompactorGroupId.of("G1");
    ReferencedTabletFile tmp1 =
        ReferencedTabletFile.of(new Path("file:///accumulo/tables/1/default_tablet/C00005.rf_tmp"));
    CompactionMetadata cm1 = new CompactionMetadata(Set.of(file1, file2), tmp1, "localhost:4444",
        CompactionKind.SYSTEM, (short) 5, cgid, false, null);

    ReferencedTabletFile tmp2 =
        ReferencedTabletFile.of(new Path("file:///accumulo/tables/1/default_tablet/C00006.rf_tmp"));
    CompactionMetadata cm2 = new CompactionMetadata(Set.of(file3), tmp2, "localhost:5555",
        CompactionKind.USER, (short) 5, cgid, false, fateId1);

    EasyMock.replay(context);

    KeyExtent extent1 = new KeyExtent(tableId1, null, null);

    var dfv = new DataFileValue(1000, 100);

    var cid1 = ExternalCompactionId.generate(UUID.randomUUID());
    var cid2 = ExternalCompactionId.generate(UUID.randomUUID());

    var selected = new SelectedFiles(Set.of(file1, file2, file3), false, fateId1);

    // should not be able to compact an offline table
    var tabletOffline = TabletMetadata.builder(new KeyExtent(tableId2, null, null))
        .putFile(file1, dfv).putFile(file2, dfv).putFile(file3, dfv).putFile(file4, dfv)
        .build(OPID, ECOMP, USER_COMPACTION_REQUESTED, SELECTED);
    assertFalse(
        canReserveCompaction(tabletOffline, CompactionKind.SYSTEM, Set.of(file1, file2), context));

    // nothing should prevent this compaction
    var tablet1 =
        TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv).putFile(file3, dfv)
            .putFile(file4, dfv).build(OPID, ECOMP, USER_COMPACTION_REQUESTED, SELECTED);
    assertTrue(canReserveCompaction(tablet1, CompactionKind.SYSTEM, Set.of(file1, file2), context));

    // should not be able to do a user compaction unless selected files are present
    assertFalse(canReserveCompaction(tablet1, CompactionKind.USER, Set.of(file1, file2), context));

    // should not be able to compact a tablet with user compaction request in place
    var tablet3 =
        TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv).putFile(file3, dfv)
            .putFile(file4, dfv).putUserCompactionRequested(fateId1).build(OPID, ECOMP, SELECTED);
    assertFalse(
        canReserveCompaction(tablet3, CompactionKind.SYSTEM, Set.of(file1, file2), context));

    // should not be able to compact a tablet when the job has files not present in the tablet
    var tablet4 = TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv)
        .putFile(file3, dfv).build(OPID, ECOMP, USER_COMPACTION_REQUESTED, SELECTED);
    assertFalse(
        canReserveCompaction(tablet4, CompactionKind.SYSTEM, Set.of(file1, file2, file4), context));

    // should not be able to compact a tablet with an operation id present
    TabletOperationId opid = TabletOperationId.from(TabletOperationType.SPLITTING, fateId1);
    var tablet5 = TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv)
        .putFile(file3, dfv).putFile(file4, dfv).putOperation(opid)
        .build(ECOMP, USER_COMPACTION_REQUESTED, SELECTED);
    assertFalse(
        canReserveCompaction(tablet5, CompactionKind.SYSTEM, Set.of(file1, file2), context));

    // should not be able to compact a tablet if the job files overlaps with running compactions
    var tablet6 = TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv)
        .putFile(file3, dfv).putFile(file4, dfv).putExternalCompaction(cid1, cm1)
        .putExternalCompaction(cid2, cm2).build(OPID, USER_COMPACTION_REQUESTED, SELECTED);
    assertFalse(
        canReserveCompaction(tablet6, CompactionKind.SYSTEM, Set.of(file1, file2), context));
    // should be able to compact the file that is outside of the set of files currently compacting
    assertTrue(canReserveCompaction(tablet6, CompactionKind.SYSTEM, Set.of(file4), context));

    // create a tablet with a selected set of files
    var selTablet = TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv)
        .putFile(file3, dfv).putFile(file4, dfv).putSelectedFiles(selected)
        .build(OPID, USER_COMPACTION_REQUESTED, ECOMP);
    // should not be able to start a system compaction if the set of files overlaps with the
    // selected files
    assertFalse(
        canReserveCompaction(selTablet, CompactionKind.SYSTEM, Set.of(file1, file2), context));
    assertFalse(
        canReserveCompaction(selTablet, CompactionKind.SYSTEM, Set.of(file3, file4), context));
    // should be able to start a system compaction on the set of files not in the selected set
    assertTrue(canReserveCompaction(selTablet, CompactionKind.SYSTEM, Set.of(file4), context));
    // should be able to start user compactions on files that are selected
    assertTrue(canReserveCompaction(selTablet, CompactionKind.USER, Set.of(file1, file2), context));
    assertTrue(canReserveCompaction(selTablet, CompactionKind.USER, Set.of(file2, file3), context));
    assertTrue(
        canReserveCompaction(selTablet, CompactionKind.USER, Set.of(file1, file2, file3), context));
    // should not be able to start user compactions on files that fall outside of the selected set
    assertFalse(
        canReserveCompaction(selTablet, CompactionKind.USER, Set.of(file1, file4), context));
    assertFalse(canReserveCompaction(selTablet, CompactionKind.USER, Set.of(file4), context));
    assertFalse(canReserveCompaction(selTablet, CompactionKind.USER,
        Set.of(file1, file2, file3, file4), context));

    // test selected files and running compaction
    var selRunningTablet = TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv)
        .putFile(file3, dfv).putFile(file4, dfv).putSelectedFiles(selected)
        .putExternalCompaction(cid2, cm2).build(OPID, USER_COMPACTION_REQUESTED);
    // should be able to compact files that are in the selected set and not in the running set
    assertTrue(
        canReserveCompaction(selRunningTablet, CompactionKind.USER, Set.of(file1, file2), context));
    // should not be able to compact because files overlap the running set
    assertFalse(
        canReserveCompaction(selRunningTablet, CompactionKind.USER, Set.of(file2, file3), context));
    // should not be able to start a system compaction if the set of files overlaps with the
    // selected files and/or the running set
    assertFalse(canReserveCompaction(selRunningTablet, CompactionKind.SYSTEM, Set.of(file1, file2),
        context));
    assertFalse(canReserveCompaction(selRunningTablet, CompactionKind.SYSTEM, Set.of(file3, file4),
        context));
    // should be able to start a system compaction on the set of files not in the selected set
    assertTrue(
        canReserveCompaction(selRunningTablet, CompactionKind.SYSTEM, Set.of(file4), context));

    // should not be able to compact a tablet that does not exists
    assertFalse(canReserveCompaction(null, CompactionKind.SYSTEM, Set.of(file1, file2), context));
    assertFalse(canReserveCompaction(null, CompactionKind.USER, Set.of(file1, file2), context));

    EasyMock.verify(context);
  }
}