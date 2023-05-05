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

package com.starrocks.connector.flink.manager.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.starrocks.connector.flink.StarRocksSinkBaseTest;
import com.starrocks.connector.flink.manager.StarRocksSinkManager;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchema.Builder;
import org.junit.Before;
import org.junit.Test;

import static com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.SINK_PARTIAL_UPDATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StarRocksSinkManagerPartialUpdateTest extends StarRocksSinkBaseTest {

    protected StarRocksSinkOptions OPTIONS;


    @Before
    public void before() {
        this.OPTIONS_BUILDER.withProperty(SINK_PARTIAL_UPDATE.key(), "true");
        this.OPTIONS_BUILDER.withProperty("sink.properties.columns", "k1,k2,v1,v2");
        this.OPTIONS = this.OPTIONS_BUILDER.build();
    }

    @Test
    public void testSetPartialUpdateButNotSetColumnMapping() {
        this.mockTableStructure();
        OPTIONS.getSinkStreamLoadProperties().remove("columns");
        assertFalse(this.OPTIONS.hasColumnMappingProperty());
        String exMsg = "";
        try {
            new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals("Column mapping must set when enable partial update.", exMsg);
        this.OPTIONS_BUILDER.withProperty("sink.properties.columns", "k1,k2,v1,v2");
        this.OPTIONS = this.OPTIONS_BUILDER.build();
    }

    @Test
    public void testValidateTableStructure() {
        this.mockTableStructure();
        this.mockStarRocksVersion("2.2.0");
        assertTrue(this.OPTIONS.hasColumnMappingProperty());
        // test succeeded
        new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
        // test failed
        new Expectations() {
            {
                StarRocksSinkManagerPartialUpdateTest.this.v.getTableColumnsMetaData();
                this.result = new ArrayList<>();
            }
        };
        String exMsg = "";
        try {
            new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(exMsg.length() > 0);
        // test failed
        new Expectations() {
            {
                StarRocksSinkManagerPartialUpdateTest.this.v.getTableColumnsMetaData();
                this.result = StarRocksSinkManagerPartialUpdateTest.this.STARROCKS_TABLE_META.keySet().stream().map(k -> new HashMap<String, Object>() {{
                    this.put("COLUMN_NAME", k);
                    this.put("COLUMN_KEY", "");
                    this.put("DATA_TYPE", "varchar");
                }}).collect(Collectors.toList());
                ;
            }
        };
        exMsg = "";
        try {
            new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals(0, exMsg.length());


        Builder schemaBuilder = this.createTableSchemaBuilder();
        schemaBuilder.field("v6", DataTypes.VARCHAR(20));
        try {
            new StarRocksSinkManager(this.OPTIONS, schemaBuilder.build());
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals("Define unknown column, should check your flink sql field. actually column is 7 flinkCols column size is 8", exMsg);

        exMsg = "";
        Builder partialSchemaBuilder = TableSchema.builder()
            .field("k1", DataTypes.TINYINT())
            .field("k2", DataTypes.VARCHAR(16))
            .field("v1", DataTypes.TIMESTAMP())
            .field("v2", DataTypes.DATE())
            .field("v3", DataTypes.DECIMAL(10, 2))
            .field("v4", DataTypes.SMALLINT());
        try {
            new StarRocksSinkManager(this.OPTIONS, partialSchemaBuilder.build());
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals("", exMsg);

        exMsg = "";
        Builder errorSchemaBuilder = TableSchema.builder()
            .field("k1", DataTypes.TINYINT())
            .field("k2", DataTypes.VARCHAR(16))
            .field("v1", DataTypes.TIMESTAMP())
            .field("v2", DataTypes.DATE())
            .field("v3", DataTypes.DECIMAL(10, 2))
            .field("v8", DataTypes.SMALLINT());
        try {
            new StarRocksSinkManager(this.OPTIONS, errorSchemaBuilder.build());
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals("Define error column [v8] they does not exist Starrocks table", exMsg);
    }


    @Test
    public void testWriteMaxBatch() throws IOException {
        this.mockTableStructure();
        this.mockStarRocksVersion(null);
        long maxRows = this.OPTIONS.getSinkMaxRows();
        this.stopHttpServer();
        {
            StarRocksSinkManager mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
            for (int i = 0; i < maxRows - 1; i++) {
                mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "test record");
            }
        }
        String exMsg = "";
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            for (int i = 0; i < maxRows * 3; i++) {
                mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "test record" + i);
            }
            mgr.flush(null, true);
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(0 < exMsg.length());
    }

    @Test
    public void testWriteMaxBytes() throws IOException {
        this.mockTableStructure();
        this.mockStarRocksVersion(null);
        long maxSize = this.OPTIONS.getSinkMaxBytes();
        this.stopHttpServer();
        int rowLength = 100000;
        {
            StarRocksSinkManager mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
            for (int i = 0; i < maxSize / rowLength - 1; i++) {
                mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), new String(new char[rowLength]));
            }
        }
        String exMsg = "";
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            for (int i = 0; i < maxSize / rowLength + 1; i++) {
                mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), new String(new char[rowLength]));
            }
            mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), new String(new char[rowLength]));
            mgr.flush(null, true);
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(0 < exMsg.length());
    }

    @Test
    public void testWriteMaxRetries() throws IOException {
        this.mockTableStructure();
        this.mockStarRocksVersion(null);
        int maxRetries = this.OPTIONS.getSinkMaxRetries();
        if (maxRetries <= 0) {
            return;
        }
        this.stopHttpServer();
        this.mockSuccessResponse();
        String exMsg = "";
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            for (int i = 0; i < this.OPTIONS.getSinkMaxRows(); i++) {
                mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "");
            }
            mgr.flush(null, true);
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(0 < exMsg.length());
    }

    @Test
    public void testFlush() throws Exception {
        this.mockTableStructure();
        this.mockStarRocksVersion(null);
        this.mockSuccessResponse();
        String exMsg = "";
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "");
            mgr.writeRecords("db1", "table1", "");
            mgr.writeRecords("db2", "table2", "");
            mgr.writeRecords("db3", "table3", "");
            mgr.flush(null, true);
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals(0, exMsg.length());
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "");
            mgr.writeRecords("db1", "table1", "");
            mgr.writeRecords("db2", "table2", "");
            mgr.writeRecords("db3", "table3", "");
            mgr.flush(null, true);
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals(0, exMsg.length());
        this.stopHttpServer();
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "");
            mgr.writeRecords("db1", "table1", "");
            mgr.writeRecords("db2", "table2", "");
            mgr.writeRecords("db3", "table3", "");
            mgr.flush(null, true);
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(0 < exMsg.length());
    }

    @Test
    public void testClose() throws Exception {
        this.mockTableStructure();
        this.mockStarRocksVersion(null);
        this.mockSuccessResponse();
        String exMsg = "";

        // test close
        StarRocksSinkManager mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
        try {
            mgr.startAsyncFlushing();
            mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "");
            mgr.flush(null, true);
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals(0, exMsg.length());
        assertTrue((boolean) this.getPrivateFieldValue(mgr, "closed"));
        TimeUnit.MILLISECONDS.sleep(100L); // wait flush thread exit
        assertFalse((boolean) this.getPrivateFieldValue(mgr, "flushThreadAlive"));
    }

    @Test
    public void testLabelExist() {
        this.mockTableStructure();
        this.mockStarRocksVersion(null);
        this.mockLabelExistsResponse(new String[]{"PREPARE", "VISIBLE"});
        String exMsg = "";
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "");
            mgr.flush(null, true);
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals(0, exMsg.length());
    }

    @Test
    public void testOffer() throws Exception {
        this.mockTableStructure();
        this.mockStarRocksVersion(null);
        String exMsg = "";
        long offerTimeoutMs = 500L;
        this.mockWaitSuccessResponse(offerTimeoutMs + 100L);

        new MockUp<StarRocksSinkOptions>(this.OPTIONS.getClass()) {
            @Mock
            public long getSinkOfferTimeout() {
                return offerTimeoutMs;
            }

        };
        // flush cost more than offer timeout
        StarRocksSinkManager mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
        this.setPrivateFieldValue(mgr, "FLUSH_QUEUE_POLL_TIMEOUT", 10);
        try {
            mgr.startAsyncFlushing();
            mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "");
            mgr.flush(null, true);
            mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "");
            mgr.flush(null, true);
        } catch (Exception e) {
            exMsg = e.getMessage();
            e.printStackTrace();
        }
        assertTrue(0 < exMsg.length());
        assertTrue(exMsg.startsWith("Timeout while offering data to flushQueue"));


        exMsg = "";
        this.mockWaitSuccessResponse(offerTimeoutMs - 100L);
        // flush cost less than offer timeout
        mgr = new StarRocksSinkManager(this.OPTIONS, this.TABLE_SCHEMA);
        try {
            mgr.startAsyncFlushing();
            mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "");
            mgr.flush(null, true);
            mgr.writeRecords(this.OPTIONS.getDatabaseName(), this.OPTIONS.getTableName(), "");
            mgr.flush(null, true);
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals(0, exMsg.length());
        assertTrue((boolean) this.getPrivateFieldValue(mgr, "closed"));
        TimeUnit.MILLISECONDS.sleep(100L); // wait flush thread exit
        assertFalse((boolean) this.getPrivateFieldValue(mgr, "flushThreadAlive"));
    }



}
