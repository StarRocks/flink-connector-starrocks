/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.starrocks.connector.flink.manager.StarRocksSinkBufferEntity;
import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.connector.flink.manager.StarRocksStreamLoadListener;
import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.tools.EnvUtils;
import com.starrocks.connector.flink.tools.JsonWrapper;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class StarRocksDynamicSinkFunctionV2<T> extends StarRocksDynamicSinkFunctionBase<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(StarRocksDynamicSinkFunctionV2.class);

    private static final int NESTED_ROW_DATA_HEADER_SIZE = 256;

    private final StarRocksSinkOptions sinkOptions;
    private final StreamLoadManagerV2 sinkManager;
    private final StarRocksISerializer serializer;
    private final StarRocksIRowTransformer<T> rowTransformer;

    private transient volatile ListState<StarrocksSnapshotState> snapshotStates;

    private transient long restoredCheckpointId;

    private transient List<ExactlyOnceLabelGeneratorSnapshot> restoredGeneratorSnapshots;

    private transient Map<Long, List<StreamLoadSnapshot>> snapshotMap;

    private transient StarRocksStreamLoadListener streamLoadListener;

    // Only valid when using exactly-once and label prefix is set
    @Nullable
    private transient ExactlyOnceLabelGeneratorFactory exactlyOnceLabelFactory;

    @Deprecated
    private transient ListState<Map<String, StarRocksSinkBufferEntity>> legacyState;
    @Deprecated
    private transient List<StarRocksSinkBufferEntity> legacyData;

    private transient long totalReceivedRows;

    private transient JsonWrapper jsonWrapper;

    public StarRocksDynamicSinkFunctionV2(StarRocksSinkOptions sinkOptions,
                                          TableSchema schema,
                                          StarRocksIRowTransformer<T> rowTransformer) {
        this.sinkOptions = sinkOptions;
        this.rowTransformer = rowTransformer;
        StarRocksSinkTable sinkTable = StarRocksSinkTable.builder()
                .sinkOptions(sinkOptions)
                .build();
        sinkTable.validateTableStructure(sinkOptions, schema);
        // StarRocksJsonSerializer depends on SinkOptions#supportUpsertDelete which is decided in
        // StarRocksSinkTable#validateTableStructure, so create serializer after validating table structure
        this.serializer = StarRocksSerializerFactory.createSerializer(sinkOptions, schema.getFieldNames());
        rowTransformer.setStarRocksColumns(sinkTable.getFieldMapping());
        rowTransformer.setTableSchema(schema);
        this.sinkManager = new StreamLoadManagerV2(sinkOptions.getProperties(sinkTable),
                sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE);
    }

    public StarRocksDynamicSinkFunctionV2(StarRocksSinkOptions sinkOptions) {
        this.sinkOptions = sinkOptions;
        this.sinkManager = new StreamLoadManagerV2(sinkOptions.getProperties(null),
                sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE);
        this.serializer = null;
        this.rowTransformer = null;
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (serializer == null) {
            if (value instanceof StarRocksSinkRowDataWithMeta) {
                StarRocksSinkRowDataWithMeta data = (StarRocksSinkRowDataWithMeta) value;
                if (Strings.isNullOrEmpty(data.getDatabase())
                        || Strings.isNullOrEmpty(data.getTable())
                        || data.getDataRows() == null) {
                    log.warn(String.format("json row data not fulfilled. {database: %s, table: %s, dataRows: %s}",
                            data.getDatabase(), data.getTable(), Arrays.toString(data.getDataRows())));
                    return;
                }
                sinkManager.write(null, data.getDatabase(), data.getTable(), data.getDataRows());
                return;
            } else if (value instanceof StarRocksRowData) {
                StarRocksRowData data = (StarRocksRowData) value;
                if (Strings.isNullOrEmpty(data.getDatabase())
                        || Strings.isNullOrEmpty(data.getTable())
                        || data.getRow() == null) {
                    log.warn(String.format("json row data not fulfilled. {database: %s, table: %s, dataRows: %s}",
                            data.getDatabase(), data.getTable(), data.getRow()));
                    return;
                }
                sinkManager.write(data.getUniqueKey(), data.getDatabase(), data.getTable(), data.getRow());
                return;
            }
            // raw data sink
            sinkManager.write(null, sinkOptions.getDatabaseName(), sinkOptions.getTableName(), value.toString());
            return;
        }

        if (value instanceof RowData) {
            if (RowKind.UPDATE_BEFORE.equals(((RowData)value).getRowKind()) &&
                    (!sinkOptions.supportUpsertDelete() || sinkOptions.getIgnoreUpdateBefore())) {
                return;
            }
            if (RowKind.DELETE.equals(((RowData)value).getRowKind()) &&
                    (!sinkOptions.supportUpsertDelete() || sinkOptions.getIgnoreDelete())) {
                return;
            }
            if (!sinkOptions.supportUpsertDelete() && RowKind.DELETE.equals(((RowData)value).getRowKind())) {
                // let go the UPDATE_AFTER and INSERT rows for tables who have a group of `unique` or `duplicate` keys.
                return;
            }
        }
        flushLegacyData();
        String serializedValue = serializer.serialize(rowTransformer.transform(value, sinkOptions.supportUpsertDelete()));
        sinkManager.write(
                null,
                sinkOptions.getDatabaseName(),
                sinkOptions.getTableName(),
                serializedValue);

        totalReceivedRows += 1;
        if (totalReceivedRows % 100 == 1) {
            log.debug("Received raw record: {}", value);
            log.debug("Received serialized record: {}", serializedValue);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        totalReceivedRows = 0;
        if (serializer != null) {
            this.serializer.open(new StarRocksISerializer.SerializerContext(getOrCreateJsonWrapper()));
        }
        this.streamLoadListener = new StarRocksStreamLoadListener(getRuntimeContext().getMetricGroup(), sinkOptions);
        sinkManager.setStreamLoadListener(streamLoadListener);

        LabelGeneratorFactory labelGeneratorFactory;
        String labelPrefix = sinkOptions.getLabelPrefix();
        if (labelPrefix == null ||
                sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE ||
                !sinkOptions.isEnableExactlyOnceLabelGen()) {
            labelGeneratorFactory = new LabelGeneratorFactory.DefaultLabelGeneratorFactory(
                    labelPrefix == null ? "flink" : labelPrefix);
        } else {
            this.exactlyOnceLabelFactory = new ExactlyOnceLabelGeneratorFactory(
                    labelPrefix,
                    getRuntimeContext().getNumberOfParallelSubtasks(),
                    getRuntimeContext().getIndexOfThisSubtask(),
                    restoredCheckpointId);
            exactlyOnceLabelFactory.restore(restoredGeneratorSnapshots);
            labelGeneratorFactory = exactlyOnceLabelFactory;
        }
        sinkManager.setLabelGeneratorFactory(labelGeneratorFactory);

        sinkManager.init();
        if (rowTransformer != null) {
            rowTransformer.setRuntimeContext(getRuntimeContext());
            rowTransformer.setFastJsonWrapper(getOrCreateJsonWrapper());
        }

        if (sinkOptions.getSemantic() == StarRocksSinkSemantic.EXACTLY_ONCE) {
            openForExactlyOnce();
        }

        log.info("Open sink function v2. {}", EnvUtils.getGitInformation());
    }

    private void openForExactlyOnce() throws Exception {
        if (sinkOptions.isAbortLingeringTxns()) {
            LingeringTransactionAborter aborter = new LingeringTransactionAborter(
                    sinkOptions.getLabelPrefix(),
                    restoredCheckpointId,
                    getRuntimeContext().getIndexOfThisSubtask(),
                    sinkOptions.getAbortCheckNumTxns(),
                    sinkOptions.getDbTables(),
                    restoredGeneratorSnapshots,
                    sinkManager.getStreamLoader());
            aborter.execute();
        }

        notifyCheckpointComplete(Long.MAX_VALUE);
    }

    private JsonWrapper getOrCreateJsonWrapper() {
        if (jsonWrapper == null) {
            this.jsonWrapper = new JsonWrapper();
        }

        return jsonWrapper;
    }

    @Override
    public void finish() {
        sinkManager.flush();
    }

    @Override
    public void close() {
        log.info("Close sink function");
        try {
            sinkManager.flush();
        } catch (Exception e) {
            log.error("Failed to flush when closing", e);
            throw e;
        } finally {
            StreamLoadSnapshot snapshot = sinkManager.snapshot();
            sinkManager.abort(snapshot);
            sinkManager.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        sinkManager.flush();
        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE) {
            return;
        }

        StreamLoadSnapshot snapshot = sinkManager.snapshot();

        if (sinkManager.prepare(snapshot)) {
            snapshotMap.put(functionSnapshotContext.getCheckpointId(), Collections.singletonList(snapshot));

            snapshotStates.clear();
            List<ExactlyOnceLabelGeneratorSnapshot> labelSnapshots = exactlyOnceLabelFactory == null ? null
                    : exactlyOnceLabelFactory.snapshot(functionSnapshotContext.getCheckpointId());
            snapshotStates.add(StarrocksSnapshotState.of(snapshotMap, labelSnapshots));
        } else {
            sinkManager.abort(snapshot);
            throw new RuntimeException("Snapshot state failed by prepare");
        }

        if (legacyState != null) {
            legacyState.clear();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        log.info("Initialize state");
        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE) {
            return;
        }

        ListStateDescriptor<byte[]> descriptor =
                new ListStateDescriptor<>(
                        "starrocks-sink-transaction",
                        TypeInformation.of(new TypeHint<byte[]>() {})
                );

        ListState<byte[]> listState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);
        snapshotStates = new SimpleVersionedListState<>(listState, new StarRocksVersionedSerializer(getOrCreateJsonWrapper()));

        // old version
        ListStateDescriptor<Map<String, StarRocksSinkBufferEntity>> legacyDescriptor =
                new ListStateDescriptor<>(
                        "buffered-rows",
                        TypeInformation.of(new TypeHint<Map<String, StarRocksSinkBufferEntity>>(){})
                );
        legacyState = functionInitializationContext.getOperatorStateStore().getListState(legacyDescriptor);
        this.restoredCheckpointId = functionInitializationContext.getRestoredCheckpointId()
                .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        this.restoredGeneratorSnapshots = new ArrayList<>();
        this.snapshotMap = new ConcurrentHashMap<>();
        if (functionInitializationContext.isRestored()) {
            for (StarrocksSnapshotState state : snapshotStates.get()) {
                for (Map.Entry<Long, List<StreamLoadSnapshot>> entry : state.getData().entrySet()) {
                    snapshotMap.compute(entry.getKey(), (k, v) -> {
                        if (v == null) {
                            return new ArrayList<>(entry.getValue());
                        }
                        v.addAll(entry.getValue());
                        return v;
                    });
                }

                if (state.getLabelSnapshots() != null) {
                    restoredGeneratorSnapshots.addAll(state.getLabelSnapshots());
                }
            }

            legacyData = new ArrayList<>();
            for (Map<String, StarRocksSinkBufferEntity> entry : legacyState.get()) {
                legacyData.addAll(entry.values());
            }
            log.info("There are {} items from legacy state", legacyData.size());
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE) {
            return;
        }

        boolean succeed = true;
        List<Long> commitCheckpointIds = snapshotMap.keySet().stream()
                .filter(cpId -> cpId <= checkpointId)
                .sorted(Long::compare)
                .collect(Collectors.toList());

        for (Long cpId : commitCheckpointIds) {
            try {
                for (StreamLoadSnapshot snapshot : snapshotMap.get(cpId)) {
                    if (!sinkManager.commit(snapshot)) {
                        succeed = false;
                        break;
                    }
                }

                if (!succeed) {
                    throw new RuntimeException(String.format("Failed to commit some transactions for snapshot %s, " +
                            "please check taskmanager logs for details", cpId));
                }
            } catch (Exception e) {
                log.error("Failed to notify checkpoint complete, checkpoint id : {}", checkpointId, e);
                throw new RuntimeException("Failed to notify checkpoint complete for checkpoint id " + checkpointId, e);
            }

            snapshotMap.remove(cpId);
        }

        // set legacyState to null to avoid clear it in latter snapshotState
        legacyState = null;
    }

    private void flushLegacyData() {
        if (legacyData == null || legacyData.isEmpty()) {
            return;
        }

        for (StarRocksSinkBufferEntity entity : legacyData) {
            for (byte[] data : entity.getBuffer()) {
                sinkManager.write(null, entity.getDatabase(), entity.getTable(), new String(data, StandardCharsets.UTF_8));
            }
            log.info("Write {} legacy records from table '{}' of database '{}'",
                    entity.getBuffer().size(), entity.getDatabase(), entity.getTable());
        }
        legacyData.clear();
    }
}
