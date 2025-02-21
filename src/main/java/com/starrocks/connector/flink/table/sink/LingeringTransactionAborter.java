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

import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TransactionStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Abort lingering transactions according. */
public class LingeringTransactionAborter {

    private static final Logger LOG = LoggerFactory.getLogger(LingeringTransactionAborter.class);

    private final String currentLabelPrefix;
    private final long restoredCheckpointId;
    private final int subtaskIndex;
    private final int checkNumTxns;
    private final List<Tuple2<String, String>> dbTables;
    private final List<ExactlyOnceLabelGeneratorSnapshot> snapshots;
    private final StreamLoader streamLoader;

    public LingeringTransactionAborter(
            String currentLabelPrefix,
            long restoredCheckpointId,
            int subtaskIndex,
            int checkNumTxns,
            List<Tuple2<String, String>> dbTables,
            List<ExactlyOnceLabelGeneratorSnapshot> snapshots,
            StreamLoader streamLoader) {
        this.currentLabelPrefix = currentLabelPrefix;
        this.restoredCheckpointId = restoredCheckpointId;
        this.subtaskIndex = subtaskIndex;
        this.checkNumTxns = checkNumTxns;
        this.dbTables = dbTables;
        this.snapshots = snapshots;
        this.streamLoader = streamLoader;
        LOG.info("Create lingering transaction aborter, currentLabelPrefix: {}, restoredCheckpointId: {}, " +
                "subtaskIndex: {}, checkNumTxns: {}, dbTables: {}, snapshots: {}",
                currentLabelPrefix, restoredCheckpointId, subtaskIndex, checkNumTxns, dbTables, snapshots);
    }

    public void execute() throws Exception {
        Map<ExactlyOnceLabelGenerator.LabelDbTableSubtask, ExactlyOnceLabelGeneratorSnapshot> map = new LinkedHashMap<>();
        Set<String> oldLabelPrefixes = new HashSet<>();
        for (ExactlyOnceLabelGeneratorSnapshot snapshot : snapshots) {
            // sanity check that the checkpoint id of the snapshot should be the same with the restoredCheckpointId
            if (snapshot.getCheckpointId() != restoredCheckpointId) {
                LOG.warn("The checkpoint id for snapshot is not equal to the restored id, restoredCheckpointId: {}," +
                        " snapshot: {}", restoredCheckpointId, snapshot);
                continue;
            }

            ExactlyOnceLabelGenerator.LabelDbTableSubtask meta = snapshot.createLabelDbTableSubtask();
            ExactlyOnceLabelGeneratorSnapshot oldSnapshot = map.get(meta);
            // Sanity check that there should not have duplicated snapshot for a LabelDbTableSubtask
            if (oldSnapshot != null) {
                LOG.warn("Find duplicate snapshot, old snapshot: {}, new snapshot: {}", oldSnapshot, snapshot);
            }
            map.put(meta, snapshot);
            oldLabelPrefixes.add(snapshot.getLabelPrefix());
        }

        // Sanity check that the label prefix should be same for all snapshots
        if (oldLabelPrefixes.size() > 1) {
            LOG.warn("There are multiple label prefix, {}", oldLabelPrefixes);
        }
        abortSnapshotLabelPrefix(new ArrayList<>(map.values()));

        // If the current label prefix is not same as the previous, it's also possible
        // there are lingering transactions with this label prefix because the job maybe
        // fail before the first checkpoint is completed, so there is no snapshot for
        // the current label prefix
        if (currentLabelPrefix != null && !oldLabelPrefixes.isEmpty() && !oldLabelPrefixes.contains(currentLabelPrefix)) {
            abortCurrentLabelPrefix();
        }
    }

    private void abortSnapshotLabelPrefix(List<ExactlyOnceLabelGeneratorSnapshot> snapshots) throws Exception {
        for (ExactlyOnceLabelGeneratorSnapshot snapshot : snapshots) {
            // if checkNumTxns is negative, execute until finding the first txn that does not need abort
            long endId = checkNumTxns < 0 ? Long.MAX_VALUE : snapshot.getNextId() + checkNumTxns;
            for (long id = snapshot.getNextId(); id < endId; id++) {
                String label = ExactlyOnceLabelGenerator.genLabel(
                        snapshot.getLabelPrefix(), snapshot.getTable(), snapshot.getSubTaskIndex(), id);
                try {
                    boolean result = tryAbortTransaction(snapshot.getDb(), snapshot.getTable(), label);
                    if (result) {
                        LOG.info("Successful to abort transaction, label: {}, snapshot: {}", label, snapshot);
                    } else {
                        LOG.info("Transaction does not need abort, label: {}, snapshot: {}, ", label, snapshot);
                    }

                    // if checkNumTxns < 0, end up after finding the first transaction that does not need abort
                    if (checkNumTxns < 0 && !result) {
                        break;
                    }
                } catch (Exception e) {
                    String errMsg = String.format("Failed to abort transactions with label %s, the snapshot is %s",
                            label, snapshot);
                    LOG.error("{}", errMsg, e);
                    throw new Exception(errMsg, e);
                }
            }
        }
    }

    private void abortCurrentLabelPrefix() throws Exception {
        for (Tuple2<String, String> dbTable : dbTables) {
            //TODO considering rescale
            // if checkNumTxns is negative, execute until find the first txn that does not need abort
            // The start checkpoint
            long endId = checkNumTxns < 0 ? Long.MAX_VALUE : restoredCheckpointId + 1 + checkNumTxns;
            for (long id = restoredCheckpointId + 1; id < endId; id++) {
                String label = ExactlyOnceLabelGenerator.genLabel(currentLabelPrefix, dbTable.f1, subtaskIndex, id);
                try {
                    boolean result = tryAbortTransaction(dbTable.f0, dbTable.f1, label);
                    if (result) {
                        LOG.info("Successful to abort transaction, label: {}, db: {}, table: {}", label, dbTable.f0, dbTable.f1);
                    } else {
                        LOG.info("Transaction does not need abort, label: {}, db: {}, table: {}", label, dbTable.f0, dbTable.f1);
                    }

                    // if checkNumTxns < 0, end up after finding the first transaction that does not need abort
                    if (checkNumTxns < 0 && !result) {
                        break;
                    }
                } catch (Exception e) {
                    String errMsg = String.format("Failed to abort transactions with label %s, db: %s, table: %s",
                            label, dbTable.f0, dbTable.f1);
                    LOG.error("{}", errMsg, e);
                    throw new Exception(errMsg, e);
                }
            }
        }
    }

    // Try to abort the transaction with the label. Return false if the transaction does
    // not need abort, such as the transaction does not exist, or it's already been aborted,
    // and return true if the label abort successfully. An exception will be thrown if the
    // transaction need abort but fail.
    private boolean tryAbortTransaction(String db, String table, String label) throws Exception {
        TransactionStatus status;
        try {
            status = streamLoader.getLoadStatus(db, table, label);
            LOG.info("Transaction status for db: {}, table: {}, label: {}, status: {}",
                    db, table, label, status);
        } catch (Exception e) {
            String errMsg = String.format("Fail to get status of the label when trying to abort " +
                    "lingering transactions, db: %s, table: %s, label: %s", db, table, label);
            LOG.error(errMsg, e);
            throw new Exception(errMsg, e);
        }

        if (status == TransactionStatus.UNKNOWN || status == TransactionStatus.ABORTED) {
            return false;
        }

        if (status == TransactionStatus.COMMITTED || status == TransactionStatus.VISIBLE) {
            // One possible reason is that the job is restoring from
            // an earlier checkpoint rather than the newest
            String errMsg = String.format("Try to abort a finished transactions, db: %s, table: %s, " +
                    "label: %s, status: %s. The reason may be that you are restoring from an earlier " +
                    "checkpoint rather than the newest, and you can use a new sink.label-prefix, or " +
                    "report the issue", db, table, label, status);
            LOG.error(errMsg);
            throw new Exception(errMsg);
        }

        if (status != TransactionStatus.PREPARE && status != TransactionStatus.PREPARED) {
            String errMsg = String.format("The status of the transaction is not supported when trying " +
                    "to abort lingering transactions, db: %s, table: %s, label: %s, status: %s",
                    db, table, label, status);
            LOG.error(errMsg);
            throw new Exception(errMsg);
        }

        try {
            StreamLoadSnapshot.Transaction transaction = new StreamLoadSnapshot.Transaction(db, table, label);
            boolean result = streamLoader.rollback(transaction);
            if (!result) {
                // TODO rollback does not return fail reason currently
                throw new Exception(String.format("Failed to abort transaction, and please find " +
                        "the reason in the taskmanager log, db: %s, table: %s, label: %s", db, table, label));
            }
            LOG.info("Successful to abort the lingering transaction, db: {}, table: {}, label: {}",
                    db, table, label);
            return true;
        } catch (Exception e) {
            // get the label status again to make sure whether the label has been aborted
            TransactionStatus newStatus = null;
            try {
                newStatus = streamLoader.getLoadStatus(db, table, label);
                LOG.info("Transaction status after trying to abort for db: {}, table: {}, label: {}, " +
                                "status: {}", db, table, label, newStatus);
            } catch (Exception ie) {
                LOG.error("Fail to get new transaction status after failing to abort the transaction, " +
                        "db: {}, table: {}, label: {}", db, table, label, ie);
            }

            if (newStatus != TransactionStatus.UNKNOWN && newStatus != TransactionStatus.ABORTED) {
                String errMsg = String.format("Fail to abort lingering transaction, db: %s, table: %s, " +
                        "label: %s, status: %s", db, table, label, newStatus);
                LOG.error(errMsg, e);
                throw new Exception(errMsg, e);
            }

            LOG.info("Successful to abort the lingering transaction, db: {}, table: {}, label: {}, " +
                    "new status: {}, but there is an exception when abort it", db, table, label, newStatus, e);
            return true;
        }
    }
}
