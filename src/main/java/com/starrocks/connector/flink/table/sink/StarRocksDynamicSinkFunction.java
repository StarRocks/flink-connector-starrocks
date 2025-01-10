/*
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

package com.starrocks.connector.flink.table.sink;

import com.google.common.base.Strings;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.manager.StarRocksQueryVisitor;
import com.starrocks.connector.flink.manager.StarRocksSinkBufferEntity;
import com.starrocks.connector.flink.manager.StarRocksSinkManager;
import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;
import com.starrocks.connector.flink.tools.EnvUtils;
import com.starrocks.connector.flink.tools.JsonWrapper;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StarRocksDynamicSinkFunction<T> extends StarRocksDynamicSinkFunctionBase<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicSinkFunction.class);

    private StarRocksSinkManager sinkManager;
    private StarRocksIRowTransformer<T> rowTransformer;
    private StarRocksSinkOptions sinkOptions;
    private StarRocksISerializer serializer;
    private transient Counter totalInvokeRowsTime;
    private transient Counter totalInvokeRows;
    private static final String COUNTER_INVOKE_ROWS_COST_TIME = "totalInvokeRowsTimeNs";
    private static final String COUNTER_INVOKE_ROWS = "totalInvokeRows";

    // state only works with `StarRocksSinkSemantic.EXACTLY_ONCE`
    private transient ListState<Map<String, StarRocksSinkBufferEntity>> checkpointedState;

    private transient JsonWrapper jsonWrapper;

    public StarRocksDynamicSinkFunction(StarRocksSinkOptions sinkOptions, TableSchema schema, StarRocksIRowTransformer<T> rowTransformer) {
        StarRocksJdbcConnectionOptions jdbcOptions = new StarRocksJdbcConnectionOptions(sinkOptions.getJdbcUrl(), sinkOptions.getUsername(), sinkOptions.getPassword());
        StarRocksJdbcConnectionProvider jdbcConnProvider = new StarRocksJdbcConnectionProvider(jdbcOptions);
        StarRocksQueryVisitor starrocksQueryVisitor = new StarRocksQueryVisitor(jdbcConnProvider, sinkOptions.getDatabaseName(), sinkOptions.getTableName());
        this.sinkManager = new StarRocksSinkManager(sinkOptions, schema, jdbcConnProvider, starrocksQueryVisitor);

        rowTransformer.setStarRocksColumns(starrocksQueryVisitor.getFieldMapping());
        rowTransformer.setTableSchema(schema);
        this.serializer = StarRocksSerializerFactory.createSerializer(sinkOptions, schema.getFieldNames());
        this.rowTransformer = rowTransformer;
        this.sinkOptions = sinkOptions;
    }

    public StarRocksDynamicSinkFunction(StarRocksSinkOptions sinkOptions) {
        this.sinkManager = new StarRocksSinkManager(sinkOptions, null);
        this.sinkOptions = sinkOptions;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.jsonWrapper = new JsonWrapper();
        if (serializer != null) {
            this.serializer.open(new StarRocksISerializer.SerializerContext(jsonWrapper));
        }
        sinkManager.open(getRuntimeContext(), jsonWrapper);
        totalInvokeRows = getRuntimeContext().getMetricGroup().counter(COUNTER_INVOKE_ROWS);
        totalInvokeRowsTime = getRuntimeContext().getMetricGroup().counter(COUNTER_INVOKE_ROWS_COST_TIME);
        if (null != rowTransformer) {
            rowTransformer.setRuntimeContext(getRuntimeContext());
            rowTransformer.setFastJsonWrapper(jsonWrapper);
        }
        sinkManager.startScheduler();
        sinkManager.startAsyncFlushing();
        LOG.info("Open sink function. {}", EnvUtils.getGitInformation());
    }

    @Override
    public synchronized void invoke(T value, Context context) throws Exception {
        long start = System.nanoTime();
        if (StarRocksSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            flushPreviousState();
        }
        if (null == serializer) {
            if (value instanceof StarRocksSinkRowDataWithMeta) {
                StarRocksSinkRowDataWithMeta data = (StarRocksSinkRowDataWithMeta)value;
                if (Strings.isNullOrEmpty(data.getDatabase()) || Strings.isNullOrEmpty(data.getTable()) || null == data.getDataRows()) {
                    LOG.warn(String.format("json row data not fulfilled. {database: %s, table: %s, dataRows: %s}", data.getDatabase(), data.getTable(), data.getDataRows()));
                    return;
                }
                sinkManager.writeRecords(data.getDatabase(), data.getTable(), data.getDataRows());
                return;
            }
            // raw data sink
            sinkManager.writeRecords(sinkOptions.getDatabaseName(), sinkOptions.getTableName(), (String) value);
            totalInvokeRows.inc(1);
            totalInvokeRowsTime.inc(System.nanoTime() - start);
            return;
        }
        if (value instanceof RowData) {
            if (RowKind.UPDATE_BEFORE.equals(((RowData)value).getRowKind()) &&
                    (!sinkOptions.supportUpsertDelete() || sinkOptions.getIgnoreUpdateBefore())) {
                return;
            }
            if (!sinkOptions.supportUpsertDelete() && RowKind.DELETE.equals(((RowData)value).getRowKind())) {
                // let go the UPDATE_AFTER and INSERT rows for tables who have a group of `unique` or `duplicate` keys.
                return;
            }
        }
        String serializedValue = serializer.serialize(rowTransformer.transform(value, sinkOptions.supportUpsertDelete()));
        sinkManager.writeRecords(
                sinkOptions.getDatabaseName(),
                sinkOptions.getTableName(),
                serializedValue
        );
        totalInvokeRows.inc(1);
        totalInvokeRowsTime.inc(System.nanoTime() - start);
        if (totalInvokeRows.getCount() % 100 == 1) {
            LOG.debug("Received raw record: {}", value);
            LOG.debug("Received serialized record: {}", serializedValue);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (!StarRocksSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            return;
        }
        ListStateDescriptor<Map<String, StarRocksSinkBufferEntity>> descriptor =
                new ListStateDescriptor<>(
                        "buffered-rows",
                        TypeInformation.of(new TypeHint<Map<String, StarRocksSinkBufferEntity>>(){})
                );
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
    }

    @Override
    public synchronized void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (StarRocksSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            flushPreviousState();
            // save state
            checkpointedState.add(sinkManager.getBufferedBatchMap());
            return;
        }
        sinkManager.flush(null, true);
    }

    @Override
    public synchronized void finish() throws Exception {
        super.finish();
        LOG.info("StarRocks sink is draining the remaining data.");
        if (StarRocksSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            flushPreviousState();
        }
        sinkManager.flush(null, true);
    }

    @Override
    public synchronized void close() throws Exception {
        super.close();
        sinkManager.close();
    }

    private void flushPreviousState() throws Exception {
        // flush the batch saved at the previous checkpoint
        for (Map<String, StarRocksSinkBufferEntity> state : checkpointedState.get()) {
            sinkManager.setBufferedBatchMap(state);
            sinkManager.flush(null, true);
        }
        checkpointedState.clear();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
    }
}