/*
 * Copyright 2022 Feedzai
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

package com.feedzai.commons.sql.abstraction.batch;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.impl.PostgreSqlEngine;
import com.feedzai.commons.sql.abstraction.listeners.BatchListener;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Class to facilitate the parallel flushes to {@link DatabaseEngine}.
 *
 * @author Abhinav Yellanki (abhinav.yellanki@feedzai.com)
 */
public class AsyncBatch extends DefaultBatch {

    /**
     * The {@link ExecutorService} to facilitate flushing using multiple threads.
     */
    private final ExecutorService flusher;
    /**
     * The {@link ThreadLocal} database engine to support parallel flushes.
     */
    private final ThreadLocal<DatabaseEngine> databaseEngine;

    /**
     * We need to close all internal {@link DatabaseEngine}, but {@link ThreadLocal} doesn't have a
     * way to provide those for us, so we cache them here for later.
     */
    private final Collection<DatabaseEngine> createdInnerDatabaseEngines = Collections.synchronizedSet(new HashSet<>());

    private final ScheduledExecutorService scheduledExecutorService;

    private final AtomicLong totalPending;

    public AsyncBatch(final DatabaseEngine de,
                      final Supplier<PostgreSqlEngine> databaseEngineProvider,
                      final String name,
                      final int batchSize,
                      final long batchTimeout,
                      final long maxAwaitTimeShutdown,
                      final BatchListener listener,
                      final int maxFlushRetries,
                      final long flushRetryDelay,
                      final Logger confidentialLogger) {
        super(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown, listener, maxFlushRetries, flushRetryDelay, confidentialLogger);

        int nThreads;
        try {
            nThreads = Integer.parseInt(System.getenv("AB_FLUSH_THREADS"));
            this.logger.info("Running Async Batch with {} threads.", nThreads);
        } catch (Exception var16) {
            this.logger.error("Failed to read AB_FLUSH_THREADS from the environment. Falling back to 10 threads.");
            nThreads = 10;
        }

        // At this point oracle flushes 10_000 rows in 300 secs and max throughput is 2_000trx/sec.
        // In order to flush at the same rate as we receiving transactions we need 10 threads:
        // number of threads = 2_000 * 300 / 6 (scoring servers) / 10_000
        this.flusher = Executors.newFixedThreadPool(
                nThreads,
                new ThreadFactoryBuilder().setNameFormat("asyncBatch-" + name + "-%d").build()
        );
        this.databaseEngine = ThreadLocal.withInitial(databaseEngineProvider);

        this.totalPending = new AtomicLong();
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

        this.logger.trace("Scheduling a periodic report of pending events.");

        this.scheduledExecutorService.scheduleAtFixedRate(
                () -> logger.trace("Total events pending to be flushed: {}", this.totalPending.get()),
                0,1, TimeUnit.SECONDS
        );
    }

    /**
     * Overridden to provide external access to this method.
     * Implemented in the {@link AbstractBatch#start()}.
     */
    @SuppressWarnings("PMD.UselessOverridingMethod")
    @Override
    public void start() {
        super.start();
    }

    @Override
    public synchronized void destroy() {
        super.flush();
        super.destroy();

        this.flusher.shutdownNow();

        try {
            if (!flusher.awaitTermination(maxAwaitTimeShutdown, TimeUnit.MILLISECONDS)) {
                logger.warn(
                        "Could not terminate async flusher within {}",
                        DurationFormatUtils.formatDurationWords(maxAwaitTimeShutdown, true, true)
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Interrupted while waiting.", e);
        }

        logger.trace("Closing internal database connections");
        this.createdInnerDatabaseEngines.forEach(DatabaseEngine::close);
    }

    /**
     * Adds the fields to the batch.
     * If the batch is full it will be flushed asynchronously.
     *
     * @param batchEntry The batch entry.
     * @throws DatabaseEngineException If an error with the database occurs.
     */
    @Override
    public synchronized void add(final BatchEntry batchEntry) throws DatabaseEngineException {
        this.buffer.add(batchEntry);
        --this.batch;
        if (this.batch <= 0) {
            flush();
        }
    }

    /**
     * Flushes the pending batches asynchronously.
     */
    @Override
    public synchronized void flush() {
        // No-op if batch is empty
        if (batch == batchSize) {
            logger.trace("[{}] Batch empty, not flushing", name);
            return;
        }

        final List<BatchEntry> temp = copyWorkingToTemp();

        final int flushed = temp.size();
        this.totalPending.addAndGet(flushed);

        this.flusher.execute(() -> {
            flush(temp);
            this.totalPending.addAndGet(-flushed);
        });
    }

    /**
     * Flushes the given list batch entries to {@link DatabaseEngine} immediately.
     *
     * @param temp List of batch entries to be written to the data base.
     */
    protected void flush(final List<BatchEntry> temp) {
        this.lastFlush = System.currentTimeMillis();

        try {
            final long start = System.currentTimeMillis();

            final DatabaseEngine de = this.databaseEngine.get();

            this.createdInnerDatabaseEngines.add(de);

            de.beginTransaction();

            for (final BatchEntry entry : temp) {
                de.addBatch(entry.getTableName(), entry.getEntityEntry());
            }

            try {
                de.flush();
                de.commit();
                this.logger.trace(
                        "[{}] Batch flushed. Took {} ms, {} rows.",
                        this.name,
                        System.currentTimeMillis() - start,
                        temp.size()
                );
            } finally {
                if (de.isTransactionActive()) {
                    de.rollback();
                }
            }
        } catch (Exception exception) {
            this.logger.error(dev, "[{}] Error occurred while flushing.", this.name, exception);
            this.onFlushFailure(temp.toArray(new BatchEntry[0]));
        }
    }

    /**
     * Copying every {@link BatchEntry} from the buffer to temp before flushing them to the {@link DatabaseEngine}.
     *
     * @return temp The ArrayList of batch entries to be flushed.
     */
    protected synchronized List<BatchEntry> copyWorkingToTemp() {
        this.batch = this.batchSize;

        final ArrayList<BatchEntry> temp = new ArrayList<>();

        while (!this.buffer.isEmpty()) {
            final BatchEntry entry = this.buffer.poll();
            temp.add(entry);
        }
        return temp;
    }
}
