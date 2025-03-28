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
package org.apache.accumulo.core.fate;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.FAILED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.FAILED_IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.NEW;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUBMITTED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUCCESSFUL;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.UNKNOWN;
import static org.apache.accumulo.core.util.ShutdownUtil.isIOException;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.logging.FateLogger;
import org.apache.accumulo.core.manager.thrift.TFateOperation;
import org.apache.accumulo.core.util.ShutdownUtil;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Fault tolerant executor
 */
public class Fate<T> {

  private static final Logger log = LoggerFactory.getLogger(Fate.class);
  private final Logger runnerLog = LoggerFactory.getLogger(TransactionRunner.class);

  private final FateStore<T> store;
  private final T environment;
  private final ScheduledThreadPoolExecutor fatePoolWatcher;
  private final ExecutorService transactionExecutor;
  private final Set<TransactionRunner> runningTxRunners;
  private final ExecutorService deadResCleanerExecutor;

  private static final EnumSet<TStatus> FINISHED_STATES = EnumSet.of(FAILED, SUCCESSFUL, UNKNOWN);
  public static final Duration INITIAL_DELAY = Duration.ofSeconds(3);
  private static final Duration DEAD_RES_CLEANUP_DELAY = Duration.ofMinutes(3);
  private static final Duration POOL_WATCHER_DELAY = Duration.ofSeconds(30);

  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final TransferQueue<FateId> workQueue;
  private final Thread workFinder;
  private final ConcurrentLinkedQueue<Integer> idleCountHistory = new ConcurrentLinkedQueue<>();

  public enum TxInfo {
    FATE_OP, AUTO_CLEAN, EXCEPTION, TX_AGEOFF, RETURN_VALUE
  }

  public enum FateOperation {
    COMMIT_COMPACTION(null),
    NAMESPACE_CREATE(TFateOperation.NAMESPACE_CREATE),
    NAMESPACE_DELETE(TFateOperation.NAMESPACE_DELETE),
    NAMESPACE_RENAME(TFateOperation.NAMESPACE_RENAME),
    SHUTDOWN_TSERVER(null),
    SYSTEM_SPLIT(null),
    SYSTEM_MERGE(null),
    TABLE_BULK_IMPORT2(TFateOperation.TABLE_BULK_IMPORT2),
    TABLE_CANCEL_COMPACT(TFateOperation.TABLE_CANCEL_COMPACT),
    TABLE_CLONE(TFateOperation.TABLE_CLONE),
    TABLE_COMPACT(TFateOperation.TABLE_COMPACT),
    TABLE_CREATE(TFateOperation.TABLE_CREATE),
    TABLE_DELETE(TFateOperation.TABLE_DELETE),
    TABLE_DELETE_RANGE(TFateOperation.TABLE_DELETE_RANGE),
    TABLE_EXPORT(TFateOperation.TABLE_EXPORT),
    TABLE_IMPORT(TFateOperation.TABLE_IMPORT),
    TABLE_MERGE(TFateOperation.TABLE_MERGE),
    TABLE_OFFLINE(TFateOperation.TABLE_OFFLINE),
    TABLE_ONLINE(TFateOperation.TABLE_ONLINE),
    TABLE_RENAME(TFateOperation.TABLE_RENAME),
    TABLE_SPLIT(TFateOperation.TABLE_SPLIT),
    TABLE_TABLET_AVAILABILITY(TFateOperation.TABLE_TABLET_AVAILABILITY);

    private final TFateOperation top;
    private static final EnumSet<FateOperation> nonThriftOps =
        EnumSet.of(COMMIT_COMPACTION, SHUTDOWN_TSERVER, SYSTEM_SPLIT, SYSTEM_MERGE);

    FateOperation(TFateOperation top) {
      this.top = top;
    }

    public static FateOperation fromThrift(TFateOperation top) {
      return FateOperation.valueOf(top.name());
    }

    public static EnumSet<FateOperation> getNonThriftOps() {
      return nonThriftOps;
    }

    public TFateOperation toThrift() {
      if (top == null) {
        throw new IllegalStateException(this + " does not have an equivalent thrift form");
      }
      return top;
    }
  }

  /**
   * A single thread that finds transactions to work on and queues them up. Do not want each worker
   * thread going to the store and looking for work as it would place more load on the store.
   */
  private class WorkFinder implements Runnable {

    @Override
    public void run() {
      while (keepRunning.get()) {
        try {
          store.runnable(keepRunning, fateId -> {
            while (keepRunning.get()) {
              try {
                // The reason for calling transfer instead of queueing is avoid rescanning the
                // storage layer and adding the same thing over and over. For example if all threads
                // were busy, the queue size was 100, and there are three runnable things in the
                // store. Do not want to keep scanning the store adding those same 3 runnable things
                // until the queue is full.
                if (workQueue.tryTransfer(fateId, 100, MILLISECONDS)) {
                  break;
                }
              } catch (InterruptedException e) {
                throw new IllegalStateException(e);
              }
            }
          });
        } catch (Exception e) {
          if (keepRunning.get()) {
            log.warn("Failure while attempting to find work for fate", e);
          } else {
            log.debug("Failure while attempting to find work for fate", e);
          }

          workQueue.clear();
        }
      }
    }
  }

  private class TransactionRunner implements Runnable {
    // used to signal a TransactionRunner to stop in the case where there are too many running
    // i.e., the property for the pool size decreased and we have excess TransactionRunners
    private final AtomicBoolean stop = new AtomicBoolean(false);

    private Optional<FateTxStore<T>> reserveFateTx() throws InterruptedException {
      while (keepRunning.get() && !stop.get()) {
        FateId unreservedFateId = workQueue.poll(100, MILLISECONDS);

        if (unreservedFateId == null) {
          continue;
        }
        var optionalopStore = store.tryReserve(unreservedFateId);
        if (optionalopStore.isPresent()) {
          return optionalopStore;
        }
      }

      return Optional.empty();
    }

    @Override
    public void run() {
      runningTxRunners.add(this);
      try {
        while (keepRunning.get() && !stop.get()) {
          FateTxStore<T> txStore = null;
          ExecutionState state = new ExecutionState();
          try {
            var optionalopStore = reserveFateTx();
            if (optionalopStore.isPresent()) {
              txStore = optionalopStore.orElseThrow();
            } else {
              continue;
            }
            state.status = txStore.getStatus();
            state.op = txStore.top();
            if (state.status == FAILED_IN_PROGRESS) {
              processFailed(txStore, state.op);
            } else if (state.status == SUBMITTED || state.status == IN_PROGRESS) {
              try {
                execute(txStore, state);
                if (state.op != null && state.deferTime != 0) {
                  // The current op is not ready to execute
                  continue;
                }
              } catch (StackOverflowException e) {
                // the op that failed to push onto the stack was never executed, so no need to undo
                // it just transition to failed and undo the ops that executed
                transitionToFailed(txStore, e);
                continue;
              } catch (Exception e) {
                blockIfHadoopShutdown(txStore.getID(), e);
                transitionToFailed(txStore, e);
                continue;
              }

              if (state.op == null) {
                // transaction is finished
                String ret = state.prevOp.getReturn();
                if (ret != null) {
                  txStore.setTransactionInfo(TxInfo.RETURN_VALUE, ret);
                }
                txStore.setStatus(SUCCESSFUL);
                doCleanUp(txStore);
              }
            }
          } catch (Exception e) {
            runnerLog.error("Uncaught exception in FATE runner thread.", e);
          } finally {
            if (txStore != null) {
              txStore.unreserve(Duration.ofMillis(state.deferTime));
            }
          }
        }
      } finally {
        log.trace("A TransactionRunner is exiting...");
        Preconditions.checkState(runningTxRunners.remove(this));
      }
    }

    private class ExecutionState {
      Repo<T> prevOp = null;
      Repo<T> op = null;
      long deferTime = 0;
      TStatus status;
    }

    // Executes as many steps of a fate operation as possible
    private void execute(final FateTxStore<T> txStore, final ExecutionState state)
        throws Exception {
      while (state.op != null && state.deferTime == 0) {
        state.deferTime = executeIsReady(txStore.getID(), state.op);

        if (state.deferTime == 0) {
          if (state.status == SUBMITTED) {
            txStore.setStatus(IN_PROGRESS);
            state.status = IN_PROGRESS;
          }

          state.prevOp = state.op;
          state.op = executeCall(txStore.getID(), state.op);

          if (state.op != null) {
            // persist the completion of this step before starting to run the next so in the case of
            // process death the completed steps are not rerun
            txStore.push(state.op);
          }
        }
      }
    }

    /**
     * The Hadoop Filesystem registers a java shutdown hook that closes the file system. This can
     * cause threads to get spurious IOException. If this happens, instead of failing a FATE
     * transaction just wait for process to die. When the manager start elsewhere the FATE
     * transaction can resume.
     */
    private void blockIfHadoopShutdown(FateId fateId, Exception e) {
      if (ShutdownUtil.isShutdownInProgress()) {

        if (e instanceof AcceptableException) {
          log.debug("Ignoring exception possibly caused by Hadoop Shutdown hook. {} ", fateId, e);
        } else if (isIOException(e)) {
          log.info("Ignoring exception likely caused by Hadoop Shutdown hook. {} ", fateId, e);
        } else {
          // sometimes code will catch an IOException caused by the hadoop shutdown hook and throw
          // another exception without setting the cause.
          log.warn("Ignoring exception possibly caused by Hadoop Shutdown hook. {} ", fateId, e);
        }

        while (true) {
          // Nothing is going to work well at this point, so why even try. Just wait for the end,
          // preventing this FATE thread from processing further work and likely failing.
          sleepUninterruptibly(1, MINUTES);
        }
      }
    }

    private void transitionToFailed(FateTxStore<T> txStore, Exception e) {
      final String msg = "Failed to execute Repo " + txStore.getID();
      // Certain FATE ops that throw exceptions don't need to be propagated up to the Monitor
      // as a warning. They're a normal, handled failure condition.
      if (e instanceof AcceptableException) {
        var tableOpEx = (AcceptableThriftTableOperationException) e;
        log.info("{} for table:{}({}) saw acceptable exception: {}", msg, tableOpEx.getTableName(),
            tableOpEx.getTableId(), tableOpEx.getDescription());
      } else {
        log.warn(msg, e);
      }
      txStore.setTransactionInfo(TxInfo.EXCEPTION, e);
      txStore.setStatus(FAILED_IN_PROGRESS);
      log.info("Updated status for Repo with {} to FAILED_IN_PROGRESS", txStore.getID());
    }

    private void processFailed(FateTxStore<T> txStore, Repo<T> op) {
      while (op != null) {
        undo(txStore.getID(), op);

        txStore.pop();
        op = txStore.top();
      }

      txStore.setStatus(FAILED);
      doCleanUp(txStore);
    }

    private void doCleanUp(FateTxStore<T> txStore) {
      Boolean autoClean = (Boolean) txStore.getTransactionInfo(TxInfo.AUTO_CLEAN);
      if (autoClean != null && autoClean) {
        txStore.delete();
      } else {
        // no longer need persisted operations, so delete them to save space in case
        // TX is never cleaned up...
        while (txStore.top() != null) {
          txStore.pop();
        }
      }
    }

    private void undo(FateId fateId, Repo<T> op) {
      try {
        op.undo(fateId, environment);
      } catch (Exception e) {
        log.warn("Failed to undo Repo, " + fateId, e);
      }
    }

    private boolean flagStop() {
      return stop.compareAndSet(false, true);
    }

    private boolean isFlaggedToStop() {
      return stop.get();
    }

  }

  protected long executeIsReady(FateId fateId, Repo<T> op) throws Exception {
    var startTime = Timer.startNew();
    var deferTime = op.isReady(fateId, environment);
    log.debug("Running {}.isReady() {} took {} ms and returned {}", op.getName(), fateId,
        startTime.elapsed(MILLISECONDS), deferTime);
    return deferTime;
  }

  protected Repo<T> executeCall(FateId fateId, Repo<T> op) throws Exception {
    var startTime = Timer.startNew();
    var next = op.call(fateId, environment);
    log.debug("Running {}.call() {} took {} ms and returned {}", op.getName(), fateId,
        startTime.elapsed(MILLISECONDS), next == null ? "null" : next.getName());

    return next;
  }

  /**
   * A thread that finds reservations held by dead processes and unreserves them
   */
  private class DeadReservationCleaner implements Runnable {
    @Override
    public void run() {
      if (keepRunning.get()) {
        store.deleteDeadReservations();
      }
    }
  }

  /**
   * Creates a Fault-tolerant executor.
   *
   * @param toLogStrFunc A function that converts Repo to Strings that are suitable for logging
   */
  public Fate(T environment, FateStore<T> store, boolean runDeadResCleaner,
      Function<Repo<T>,String> toLogStrFunc, AccumuloConfiguration conf) {
    this.store = FateLogger.wrap(store, toLogStrFunc, false);
    this.environment = environment;
    final ThreadPoolExecutor pool = ThreadPools.getServerThreadPools().createExecutorService(conf,
        Property.MANAGER_FATE_THREADPOOL_SIZE, true);
    this.workQueue = new LinkedTransferQueue<>();
    this.runningTxRunners = Collections.synchronizedSet(new HashSet<>());
    this.fatePoolWatcher =
        ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(conf);
    ThreadPools.watchCriticalScheduledTask(fatePoolWatcher.scheduleWithFixedDelay(() -> {
      // resize the pool if the property changed
      ThreadPools.resizePool(pool, conf, Property.MANAGER_FATE_THREADPOOL_SIZE);
      final int configured = conf.getCount(Property.MANAGER_FATE_THREADPOOL_SIZE);
      final int needed = configured - runningTxRunners.size();
      if (needed > 0) {
        // If the pool grew, then ensure that there is a TransactionRunner for each thread
        for (int i = 0; i < needed; i++) {
          try {
            pool.execute(new TransactionRunner());
          } catch (RejectedExecutionException e) {
            // RejectedExecutionException could be shutting down
            if (pool.isShutdown()) {
              // The exception is expected in this case, no need to spam the logs.
              log.trace("Error adding transaction runner to FaTE executor pool.", e);
            } else {
              // This is bad, FaTE may no longer work!
              log.error("Error adding transaction runner to FaTE executor pool.", e);
            }
            break;
          }
        }
        idleCountHistory.clear();
      } else if (needed < 0) {
        // If we need the pool to shrink, then ensure excess TransactionRunners are safely stopped.
        // Flag the necessary number of TransactionRunners to safely stop when they are done work
        // on a transaction.
        int numFlagged =
            (int) runningTxRunners.stream().filter(TransactionRunner::isFlaggedToStop).count();
        int numToStop = -1 * (numFlagged + needed);
        for (var runner : runningTxRunners) {
          if (numToStop <= 0) {
            break;
          }
          if (runner.flagStop()) {
            log.trace("Flagging a TransactionRunner to stop...");
            numToStop--;
          }
        }
      } else {
        // The property did not change, but should it based on idle Fate threads? Maintain
        // count of the last X minutes of idle Fate threads. If zero 95% of the time, then suggest
        // that the MANAGER_FATE_THREADPOOL_SIZE be increased.
        final long interval = Math.min(60, TimeUnit.MILLISECONDS
            .toMinutes(conf.getTimeInMillis(Property.MANAGER_FATE_IDLE_CHECK_INTERVAL)));
        if (interval == 0) {
          idleCountHistory.clear();
        } else {
          if (idleCountHistory.size() >= interval * 2) { // this task runs every 30s
            int zeroFateThreadsIdleCount = 0;
            for (Integer idleConsumerCount : idleCountHistory) {
              if (idleConsumerCount == 0) {
                zeroFateThreadsIdleCount++;
              }
            }
            boolean needMoreThreads =
                (zeroFateThreadsIdleCount / (double) idleCountHistory.size()) >= 0.95;
            if (needMoreThreads) {
              log.warn(
                  "All Fate threads appear to be busy for the last {} minutes,"
                      + " consider increasing property: {}",
                  interval, Property.MANAGER_FATE_THREADPOOL_SIZE.getKey());
              // Clear the history so that we don't log for interval minutes.
              idleCountHistory.clear();
            } else {
              while (idleCountHistory.size() >= interval * 2) {
                idleCountHistory.remove();
              }
            }
          }
          idleCountHistory.add(workQueue.getWaitingConsumerCount());
        }
      }
    }, INITIAL_DELAY.toSeconds(), getPoolWatcherDelay().toSeconds(), SECONDS));
    this.transactionExecutor = pool;

    ScheduledExecutorService deadResCleanerExecutor = null;
    if (runDeadResCleaner) {
      // Create a dead reservation cleaner for this store that will periodically clean up
      // reservations held by dead processes, if they exist.
      deadResCleanerExecutor = ThreadPools.getServerThreadPools().createScheduledExecutorService(1,
          store.type() + "-dead-reservation-cleaner-pool");
      ScheduledFuture<?> deadReservationCleaner =
          deadResCleanerExecutor.scheduleWithFixedDelay(new DeadReservationCleaner(),
              INITIAL_DELAY.toSeconds(), getDeadResCleanupDelay().toSeconds(), SECONDS);
      ThreadPools.watchCriticalScheduledTask(deadReservationCleaner);
    }
    this.deadResCleanerExecutor = deadResCleanerExecutor;

    this.workFinder = Threads.createThread("Fate work finder", new WorkFinder());
    this.workFinder.start();
  }

  public Duration getDeadResCleanupDelay() {
    return DEAD_RES_CLEANUP_DELAY;
  }

  public Duration getPoolWatcherDelay() {
    return POOL_WATCHER_DELAY;
  }

  @VisibleForTesting
  public int getTxRunnersActive() {
    return runningTxRunners.size();
  }

  // get a transaction id back to the requester before doing any work
  public FateId startTransaction() {
    return store.create();
  }

  public void seedTransaction(FateOperation fateOp, FateKey fateKey, Repo<T> repo,
      boolean autoCleanUp) {
    try (var seeder = store.beginSeeding()) {
      @SuppressWarnings("unused")
      var unused = seeder.attemptToSeedTransaction(fateOp, fateKey, repo, autoCleanUp);
    }
  }

  // start work in the transaction.. it is safe to call this
  // multiple times for a transaction... but it will only seed once
  public void seedTransaction(FateOperation fateOp, FateId fateId, Repo<T> repo,
      boolean autoCleanUp, String goalMessage) {
    log.info("Seeding {} {}", fateId, goalMessage);
    store.seedTransaction(fateOp, fateId, repo, autoCleanUp);
  }

  // check on the transaction
  public TStatus waitForCompletion(FateId fateId) {
    return store.read(fateId).waitForStatusChange(FINISHED_STATES);
  }

  /**
   * Attempts to cancel a running Fate transaction
   *
   * @param fateId fate transaction id
   * @return true if transaction transitioned to a failed state or already in a completed state,
   *         false otherwise
   */
  public boolean cancel(FateId fateId) {
    for (int retries = 0; retries < 5; retries++) {
      Optional<FateTxStore<T>> optionalTxStore = store.tryReserve(fateId);
      if (optionalTxStore.isPresent()) {
        var txStore = optionalTxStore.orElseThrow();
        try {
          TStatus status = txStore.getStatus();
          log.info("status is: {}", status);
          if (status == NEW || status == SUBMITTED) {
            txStore.setTransactionInfo(TxInfo.EXCEPTION, new TApplicationException(
                TApplicationException.INTERNAL_ERROR, "Fate transaction cancelled by user"));
            txStore.setStatus(FAILED_IN_PROGRESS);
            log.info("Updated status for {} to FAILED_IN_PROGRESS because it was cancelled by user",
                fateId);
            return true;
          } else {
            log.info("{} cancelled by user but already in progress or finished state", fateId);
            return false;
          }
        } finally {
          txStore.unreserve(Duration.ZERO);
        }
      } else {
        // reserved, lets retry.
        UtilWaitThread.sleep(500);
      }
    }
    log.info("Unable to reserve transaction {} to cancel it", fateId);
    return false;
  }

  // resource cleanup
  public void delete(FateId fateId) {
    FateTxStore<T> txStore = store.reserve(fateId);
    try {
      switch (txStore.getStatus()) {
        case NEW:
        case SUBMITTED:
        case FAILED:
        case SUCCESSFUL:
          txStore.delete();
          break;
        case FAILED_IN_PROGRESS:
        case IN_PROGRESS:
          throw new IllegalStateException("Can not delete in progress transaction " + fateId);
        case UNKNOWN:
          // nothing to do, it does not exist
          break;
      }
    } finally {
      txStore.unreserve(Duration.ZERO);
    }
  }

  public String getReturn(FateId fateId) {
    FateTxStore<T> txStore = store.reserve(fateId);
    try {
      if (txStore.getStatus() != SUCCESSFUL) {
        throw new IllegalStateException(
            "Tried to get exception when transaction " + fateId + " not in successful state");
      }
      return (String) txStore.getTransactionInfo(TxInfo.RETURN_VALUE);
    } finally {
      txStore.unreserve(Duration.ZERO);
    }
  }

  // get reportable failures
  public Exception getException(FateId fateId) {
    FateTxStore<T> txStore = store.reserve(fateId);
    try {
      if (txStore.getStatus() != FAILED) {
        throw new IllegalStateException(
            "Tried to get exception when transaction " + fateId + " not in failed state");
      }
      return (Exception) txStore.getTransactionInfo(TxInfo.EXCEPTION);
    } finally {
      txStore.unreserve(Duration.ZERO);
    }
  }

  /**
   * Lists transctions for a given fate key type.
   */
  public Stream<FateKey> list(FateKey.FateKeyType type) {
    return store.list(type);
  }

  /**
   * Initiates shutdown of background threads and optionally waits on them.
   */
  public void shutdown(long timeout, TimeUnit timeUnit) {
    if (keepRunning.compareAndSet(true, false)) {
      fatePoolWatcher.shutdown();
      transactionExecutor.shutdown();
      workFinder.interrupt();
      if (deadResCleanerExecutor != null) {
        deadResCleanerExecutor.shutdown();
      }
    }

    if (timeout > 0) {
      long start = System.nanoTime();

      while ((System.nanoTime() - start) < timeUnit.toNanos(timeout) && (workFinder.isAlive()
          || !transactionExecutor.isTerminated() || !fatePoolWatcher.isTerminated()
          || (deadResCleanerExecutor != null && !deadResCleanerExecutor.isTerminated()))) {
        try {
          if (!fatePoolWatcher.awaitTermination(1, SECONDS)) {
            log.debug("Fate {} is waiting for pool watcher to terminate", store.type());
            continue;
          }

          if (!transactionExecutor.awaitTermination(1, SECONDS)) {
            log.debug("Fate {} is waiting for worker threads to terminate", store.type());
            continue;
          }

          if (deadResCleanerExecutor != null
              && !deadResCleanerExecutor.awaitTermination(1, SECONDS)) {
            log.debug("Fate {} is waiting for dead reservation cleaner thread to terminate",
                store.type());
            continue;
          }

          workFinder.join(1_000);
          if (workFinder.isAlive()) {
            log.debug("Fate {} is waiting for work finder thread to terminate", store.type());
            workFinder.interrupt();
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      if (workFinder.isAlive() || !transactionExecutor.isTerminated()
          || (deadResCleanerExecutor != null && !deadResCleanerExecutor.isTerminated())) {
        log.warn(
            "Waited for {}ms for all fate {} background threads to stop, but some are still running. workFinder:{} transactionExecutor:{} deadResCleanerExecutor:{}",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), store.type(),
            workFinder.isAlive(), !transactionExecutor.isTerminated(),
            (deadResCleanerExecutor != null && !deadResCleanerExecutor.isTerminated()));
      }
    }

    // interrupt the background threads
    transactionExecutor.shutdownNow();
    if (deadResCleanerExecutor != null) {
      deadResCleanerExecutor.shutdownNow();
    }
    idleCountHistory.clear();
  }
}
