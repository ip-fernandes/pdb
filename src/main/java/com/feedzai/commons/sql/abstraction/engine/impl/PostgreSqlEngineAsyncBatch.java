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

package com.feedzai.commons.sql.abstraction.engine.impl;

import com.feedzai.commons.sql.abstraction.batch.AbstractBatch;
import com.feedzai.commons.sql.abstraction.batch.AsyncBatch;
import com.feedzai.commons.sql.abstraction.batch.DefaultBatch;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.MappedEntity;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.listeners.BatchListener;
import org.slf4j.Logger;

import java.util.function.Supplier;

import static com.feedzai.commons.sql.abstraction.batch.AbstractBatch.DEFAULT_RETRY_INTERVAL;
import static com.feedzai.commons.sql.abstraction.batch.AbstractBatch.NO_RETRY;

/**
 * Oracle Engine to create {@link AsyncBatch} instead of {@link DefaultBatch}.
 *
 * @author Abhinav Yellanki (abhinav.yellanki@feedzai.com)
 */
public class PostgreSqlEngineAsyncBatch extends PostgreSqlEngine {

    private final Supplier<PostgreSqlEngine> databaseEngineProvider;

    /**
     * Creating a OracleEngineAsyncBatch which allows to run the parallel asynchronous flushes.
     *
     * @param properties The {@link PdbProperties} param.
     * @throws DatabaseEngineException Throws an exception if it fails to create the {@link DatabaseEngine}.
     */
    public PostgreSqlEngineAsyncBatch(final PdbProperties properties) throws DatabaseEngineException {
        super(properties);

        databaseEngineProvider = () -> {
            try {
                final PostgreSqlEngine postgreSqlEngine = new PostgreSqlEngine(properties);

                // FIXME - this will be out of sync if someone adds more entity after creating the batch
                for (final MappedEntity mappedEntity : this.entities.values()) {
                    postgreSqlEngine.loadEntity(mappedEntity.getEntity());
                }

                return postgreSqlEngine;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        };
    }

    @Override
    public AbstractBatch createBatch(final int batchSize,
                                     final long batchTimeout,
                                     final String batchName,
                                     final BatchListener batchListener,
                                     final Logger confidentialLogger) {
        final AsyncBatch batch = new AsyncBatch(
                this,
                databaseEngineProvider,
                batchName,
                batchSize,
                batchTimeout,
                properties.getMaximumAwaitTimeBatchShutdown(),
                batchListener,
                NO_RETRY,
                DEFAULT_RETRY_INTERVAL,
                confidentialLogger
        );
        batch.start();

        return batch;
    }

}
