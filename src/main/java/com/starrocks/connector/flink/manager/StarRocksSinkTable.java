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

package com.starrocks.connector.flink.manager;

import com.google.common.collect.Ordering;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
public class StarRocksSinkTable {

    private final String database;
    private final String table;

    private volatile String version;

    private final StarRocksQueryVisitor starRocksQueryVisitor;

    // Whether the columns in the schema of flink and starrocks are aligned, that's, they
    // have the same number of columns, column names and positions are the same, and the
    // types are compatible. false indicates that they are not aligned or unknown. Note
    // that primary key table has an implicit __op column when loading, so flink and starrocks
    // schema always are unaligned for primary key table.
    private boolean flinkAndStarRocksSchemaAligned = false;

    private StarRocksSinkTable(Builder builder) {
        this.database = builder.database;
        this.table = builder.table;

        StarRocksJdbcConnectionOptions options =
                new StarRocksJdbcConnectionOptions(builder.jdbcUrl, builder.username, builder.password);
        StarRocksJdbcConnectionProvider jdbcConnectionProvider = new StarRocksJdbcConnectionProvider(options);
        this.starRocksQueryVisitor = new StarRocksQueryVisitor(jdbcConnectionProvider, builder.database, builder.table);
    }

    public static StarRocksSinkTable.Builder builder() {
        return new StarRocksSinkTable.Builder();
    }

    public boolean isFlinkAndStarRocksColumnsAligned() {
        return flinkAndStarRocksSchemaAligned;
    }

    public String getVersion() {
        if (version == null) {
            synchronized (this) {
                if (version == null) {
                    version = starRocksQueryVisitor.getStarRocksVersion();
                }
            }
        }
        return version;
    }

    public boolean isOpAutoProjectionInJson() {
        return version == null || version.length() > 0 && !version.trim().startsWith("1.");
    }

    public Map<String, StarRocksDataType> getFieldMapping() {
        return starRocksQueryVisitor.getFieldMapping();
    }

    public void validateTableStructure(StarRocksSinkOptions sinkOptions, TableSchema flinkSchema) {
        if (flinkSchema == null) {
            return;
        }

        // 1. verify the pk constraint if it's a primary key table
        Optional<UniqueConstraint> constraint = flinkSchema.getPrimaryKey();
        List<Map<String, Object>> rows = starRocksQueryVisitor.getTableColumnsMetaData();
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("Couldn't get the sink table's column info.");
        }
        // validate primary keys
        List<String> primaryKeys = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            String keysType = row.get("COLUMN_KEY").toString();
            if (!"PRI".equals(keysType)) {
                continue;
            }
            primaryKeys.add(row.get("COLUMN_NAME").toString().toLowerCase());
        }
        if (!primaryKeys.isEmpty()) {
            if (!constraint.isPresent()) {
                throw new IllegalArgumentException("Primary keys not defined in the sink `TableSchema`.");
            }
            if (constraint.get().getColumns().size() != primaryKeys.size() ||
                    !constraint.get().getColumns().stream().allMatch(col -> primaryKeys.contains(col.toLowerCase()))) {
                throw new IllegalArgumentException("Primary keys of the flink `TableSchema` do not match with the ones from starrocks table.");
            }
            sinkOptions.enableUpsertDelete();
        }

        if (sinkOptions.hasColumnMappingProperty()) {
            return;
        }

        // 2. verify the columns of flink are contained in the starrocks schema
        // and the type is compatible
        Map<String, Map<String, Object>> starrocksColumnMapping = new HashMap<>();
        for (Map<String, Object> row : rows) {
            String name = row.get("COLUMN_NAME").toString().toLowerCase();
            starrocksColumnMapping.put(name, row);
        }

        // the position where a flink column is in the schema of starrocks
        List<Long> columnPositionInStarRocksSchema = new ArrayList<>();
        for (TableColumn column : flinkSchema.getTableColumns()) {
            Map<String, Object> srColumn = starrocksColumnMapping.get(column.getName().toLowerCase());
            if (srColumn == null) {
                throw new IllegalArgumentException("StarRocks does not have column " + column.getName());
            }

            String srType = srColumn.get("DATA_TYPE").toString().toLowerCase();
            // Some types of StarRocks, such as json, are not mapped to Flink natively,
            // and there will be no entry in typesMap, but they can be represented as
            // STRING in Flink generally, so we think the type is matched even if
            // typesMap does not contain the srType
            boolean typeMatched = !typesMap.containsKey(srType) || typesMap.get(srType).contains(column.getType().getLogicalType().getTypeRoot());
            if (!typeMatched) {
                throw new IllegalArgumentException(
                        String.format("Flink and StarRocks types are not matched for column %s, " +
                                "flink type is %s, starrocks type is %s", column.getName(), column.getType(), srType));
            }
            columnPositionInStarRocksSchema.add((long) srColumn.get("ORDINAL_POSITION"));
        }
        sinkOptions.setTableSchemaFieldNames(flinkSchema.getFieldNames());

        // 3. decide whether the schemas of flink and starrocks are aligned
        if (!primaryKeys.isEmpty()) {
            // always unaligned for primary key tale
            flinkAndStarRocksSchemaAligned = false;
        } else {
            // aligned if the number of columns is same, and the positions are ordered
            flinkAndStarRocksSchemaAligned = flinkSchema.getTableColumns().size() == starrocksColumnMapping.size() &&
                    Ordering.natural().isOrdered(columnPositionInStarRocksSchema);
        }
    }

    public static class Builder {
        private String database;
        private String table;
        private String jdbcUrl;
        private String username;
        private String password;

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder sinkOptions(StarRocksSinkOptions sinkOptions) {
            this.jdbcUrl = sinkOptions.getJdbcUrl();
            this.username = sinkOptions.getUsername();
            this.password = sinkOptions.getPassword();
            this.database = sinkOptions.getDatabaseName();
            this.table = sinkOptions.getTableName();
            return this;
        }

        public StarRocksSinkTable build() {
            return new StarRocksSinkTable(this);
        }
    }

    private static final Map<String, List<LogicalTypeRoot>> typesMap = new HashMap<>();

    static {
        typesMap.put("bigint", Arrays.asList(LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("largeint", Arrays.asList(LogicalTypeRoot.DECIMAL, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("char", Arrays.asList(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR));
        typesMap.put("date", Arrays.asList(LogicalTypeRoot.DATE, LogicalTypeRoot.VARCHAR));
        typesMap.put("datetime", Arrays.asList(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, LogicalTypeRoot.VARCHAR));
        typesMap.put("decimal", Arrays.asList(LogicalTypeRoot.DECIMAL, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.DOUBLE, LogicalTypeRoot.FLOAT));
        typesMap.put("double", Arrays.asList(LogicalTypeRoot.DOUBLE, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER));
        typesMap.put("float", Arrays.asList(LogicalTypeRoot.FLOAT, LogicalTypeRoot.INTEGER));
        typesMap.put("int", Arrays.asList(LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("tinyint", Arrays.asList(LogicalTypeRoot.TINYINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY, LogicalTypeRoot.BOOLEAN));
        typesMap.put("smallint", Arrays.asList(LogicalTypeRoot.SMALLINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("varchar", Arrays.asList(LogicalTypeRoot.VARCHAR, LogicalTypeRoot.ARRAY, LogicalTypeRoot.MAP, LogicalTypeRoot.ROW));
        typesMap.put("string", Arrays.asList(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR, LogicalTypeRoot.ARRAY, LogicalTypeRoot.MAP, LogicalTypeRoot.ROW));
        typesMap.put("bigint unsigned", Arrays.asList(LogicalTypeRoot.DECIMAL));
    }

}
