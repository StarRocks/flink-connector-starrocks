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

package com.starrocks.connector.flink.it.catalog;

import com.starrocks.connector.flink.catalog.StarRocksCatalog;
import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import com.starrocks.connector.flink.it.StarRocksITTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Tests for {@link StarRocksCatalog}. */
public class StarRocksCatalogTest extends StarRocksITTestBase {

    private String tableName;
    private StarRocksCatalog catalog;

    @Before
    public void setup() throws Exception {
        this.tableName = "test_catalog_" + genRandomUuid();
        this.catalog = new StarRocksCatalog(getJdbcUrl(), USERNAME, PASSWORD);
        this.catalog.open();
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 FLOAT," +
                                "c2 BOOLEAN," +
                                "c3 DATE" +
                                ") ENGINE = OLAP " +
                                "PRIMARY KEY(c0) " +
                                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
    }

    @After
    public void teardown() throws Exception {
        if (this.catalog != null) {
            this.catalog.close();
        }
    }

    @Test
    public void testAlterAddColumns() throws Exception {
        StarRocksTable oldTable = catalog.getTable(DB_NAME, tableName).orElse(null);
        assertNotNull(oldTable);
        List<StarRocksColumn> addColumns = new ArrayList<>();

        StarRocksColumn c4 = new StarRocksColumn.Builder()
                .setColumnName("c4")
                .setOrdinalPosition(4)
                .setDataType("BIGINT")
                .setNullable(true)
                .setColumnSize(19)
                .setDecimalDigits(0)
                .setColumnComment("add c4")
                .build();
        addColumns.add(c4);

        StarRocksColumn c5 = new StarRocksColumn.Builder()
                .setColumnName("c5")
                .setOrdinalPosition(5)
                .setDataType("DECIMAL")
                .setNullable(true)
                .setColumnSize(20)
                .setDecimalDigits(1)
                .setColumnComment("add c5")
                .build();
        addColumns.add(c5);

        StarRocksColumn c6 = new StarRocksColumn.Builder()
                .setColumnName("c6")
                .setOrdinalPosition(6)
                .setDataType("DATETIME")
                .setNullable(true)
                .setDefaultValue(null)
                .setColumnSize(null)
                .setDecimalDigits(null)
                .setColumnComment("add c6")
                .build();
        addColumns.add(c6);

        catalog.alterAddColumns(DB_NAME, tableName, addColumns, 60);
        StarRocksTable newTable = catalog.getTable(DB_NAME, tableName).orElse(null);
        assertNotNull(newTable);

        List<StarRocksColumn> expectedColumns = new ArrayList<>();
        expectedColumns.addAll(oldTable.getColumns());
        expectedColumns.addAll(addColumns);

        assertEquals(expectedColumns, newTable.getColumns());
    }

    @Test
    public void testAlterDropColumns() throws Exception {
        StarRocksTable oldTable = catalog.getTable(DB_NAME, tableName).orElse(null);
        assertNotNull(oldTable);
        List<String> dropColumns = Arrays.asList("c2", "c3");
        catalog.alterDropColumns(DB_NAME, tableName, dropColumns, 60);
        StarRocksTable newTable = catalog.getTable(DB_NAME, tableName).orElse(null);
        assertNotNull(newTable);

        List<StarRocksColumn> expectedColumns = oldTable.getColumns().stream()
                .filter(column -> !dropColumns.contains(column.getColumnName()))
                .collect(Collectors.toList());
        assertEquals(expectedColumns, newTable.getColumns());
    }

    @Test
    public void testAlterModifyColumns() throws Exception {
        StarRocksTable oldTable = catalog.getTable(DB_NAME, tableName).orElse(null);
        assertNotNull(oldTable);
        Map<String, String> modifyColumnsTypeMap = new HashMap<>();
        modifyColumnsTypeMap.put("c1", "DOUBLE");
        modifyColumnsTypeMap.put("c2", "BOOLEAN");
        Map<String, String> modifyColumnsPositionMap = new HashMap<>();
        modifyColumnsPositionMap.put("c1", "c3");

        catalog.alterModifyColumns(DB_NAME, tableName, modifyColumnsTypeMap, modifyColumnsPositionMap, 60);
        StarRocksTable newTable = catalog.getTable(DB_NAME, tableName).orElse(null);
        assertNotNull(newTable);

        List<StarRocksColumn> expectedColumns = Arrays.asList(
                new StarRocksColumn.Builder()
                        .setColumnName("c0")
                        .setOrdinalPosition(0)
                        .setDataType("int")
                        .setNullable(false)
                        .setColumnSize(10)
                        .setDecimalDigits(0)
                        .setColumnComment("")
                        .build(),
                new StarRocksColumn.Builder()
                        .setColumnName("c2")
                        .setOrdinalPosition(1)
                        .setDataType("boolean")
                        .setDecimalDigits(0)
                        .setNullable(true)
                        .setColumnComment("")
                        .build(),
                new StarRocksColumn.Builder()
                        .setColumnName("c3")
                        .setOrdinalPosition(2)
                        .setDataType("date")
                        .setNullable(true)
                        .setColumnComment("")
                        .build(),
                new StarRocksColumn.Builder()
                        .setColumnName("c1")
                        .setOrdinalPosition(3)
                        .setDataType("double")
                        .setColumnSize(15)
                        .setDecimalDigits(15)
                        .setNullable(true)
                        .setColumnComment("")
                        .build()
        );

        assertEquals(expectedColumns, newTable.getColumns());
    }
}
