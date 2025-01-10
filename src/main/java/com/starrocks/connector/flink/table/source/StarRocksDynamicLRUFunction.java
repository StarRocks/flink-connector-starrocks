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

package com.starrocks.connector.flink.table.source;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StarRocksDynamicLRUFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicLRUFunction.class);

    private final ColumnRichInfo[] filterRichInfos;
    private final StarRocksSourceOptions sourceOptions;
    private final ArrayList<String> filterList;
    private QueryInfo queryInfo;
    private final SelectColumn[] selectColumns;
    private final List<ColumnRichInfo> columnRichInfos;
    private List<StarRocksSourceDataReader> dataReaderList;

    private transient Cache<Row, List<RowData>> cache;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    public StarRocksDynamicLRUFunction(StarRocksSourceOptions sourceOptions,
                                       ColumnRichInfo[] filterRichInfos,
                                       List<ColumnRichInfo> columnRichInfos,
                                       SelectColumn[] selectColumns) {
        this.sourceOptions = sourceOptions;
        this.filterRichInfos = filterRichInfos;
        this.columnRichInfos = columnRichInfos;
        this.selectColumns = selectColumns;

        this.cacheMaxSize = sourceOptions.getLookupCacheMaxRows();
        this.cacheExpireMs = sourceOptions.getLookupCacheTTL();
        this.maxRetryTimes = sourceOptions.getLookupMaxRetries();

        this.filterList = new ArrayList<>();
        this.dataReaderList = new ArrayList<>();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.cache = cacheMaxSize == -1 || cacheExpireMs == -1
                ? null
                : CacheBuilder.newBuilder()
                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(cacheMaxSize)
                    .build();
    }

    public void eval(Object... keys) {
        Row keyRow = Row.of(keys);
        if (cache != null) {
            List<RowData> cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                for (RowData cachedRow : cachedRows) {
                    collect(cachedRow);
                }
                return;
            }
        }

        for (int j = 0; j < keys.length; j ++) {
            getFieldValue(keys[j], filterRichInfos[j]);
        }
        String filter = String.join(" and ", filterList);
        filterList.clear();
        String columns = Arrays.stream(selectColumns)
            .map(col -> "`" + col.getColumnName() + "`")
            .collect(Collectors.joining(","));
        String SQL = "select " + columns + " from " + sourceOptions.getDatabaseName() + "." + sourceOptions.getTableName() + " where " + filter;
        LOG.info("LookUpFunction SQL [{}]", SQL);
        this.queryInfo = StarRocksSourceCommonFunc.getQueryInfo(this.sourceOptions, SQL);
        List<List<QueryBeXTablets>> lists = StarRocksSourceCommonFunc.splitQueryBeXTablets(1, queryInfo);
        lists.get(0).forEach(beXTablets -> {
            StarRocksSourceBeReader beReader = new StarRocksSourceBeReader(beXTablets.getBeNode(),
                    columnRichInfos,
                                                                           selectColumns,
                                                                           sourceOptions);
            beReader.openScanner(beXTablets.getTabletIds(), queryInfo.getQueryPlan().getOpaqued_query_plan(), sourceOptions);
            beReader.startToRead();
            this.dataReaderList.add(beReader);
        });
        if (cache == null) {
            this.dataReaderList.parallelStream().forEach(dataReader -> {
                while (dataReader.hasNext()) {
                    RowData row = dataReader.getNext();
                    collect(row);
                }
            });
        } else {
            ArrayList<RowData> rows = new ArrayList<>();
            this.dataReaderList.parallelStream().forEach(dataReader -> {
                while (dataReader.hasNext()) {
                    RowData row = dataReader.getNext();
                    rows.add(row);
                    collect(row);
                }
            });
            rows.trimToSize();
            cache.put(keyRow, rows);
        }
    }

    private void getFieldValue(Object obj, ColumnRichInfo columnRichInfo) {
        LogicalTypeRoot flinkTypeRoot = columnRichInfo.getDataType().getLogicalType().getTypeRoot();
        String filter = "";
        if (flinkTypeRoot == LogicalTypeRoot.DATE) {
            Calendar c = Calendar.getInstance();
            c.setTime(new Date(0L));
            c.add(Calendar.DATE, (int)obj);
            Date d = c.getTime();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            filter = columnRichInfo.getColumnName() + " = '" + sdf.format(d).toString() + "'";
        }
        if (flinkTypeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE ||
            flinkTypeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE ||
            flinkTypeRoot == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE) {

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            String strDateTime = dtf.format(((TimestampData)obj).toLocalDateTime());
            filter = columnRichInfo.getColumnName() + " = '" + strDateTime + "'";
        }
        if (flinkTypeRoot == LogicalTypeRoot.CHAR ||
            flinkTypeRoot == LogicalTypeRoot.VARCHAR) {

            filter = columnRichInfo.getColumnName() + " = '" + ((BinaryStringData)obj).toString() + "'";
        }
        if (flinkTypeRoot == LogicalTypeRoot.BOOLEAN) {
            filter = columnRichInfo.getColumnName() + " = " + (boolean) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.TINYINT) {
            filter = columnRichInfo.getColumnName() + " = " + (byte) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.SMALLINT) {
            filter = columnRichInfo.getColumnName() + " = " + (short) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.INTEGER) {
            filter = columnRichInfo.getColumnName() + " = " + (int) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.BIGINT) {
            filter = columnRichInfo.getColumnName() + " = " + (long) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.FLOAT) {
            filter = columnRichInfo.getColumnName() + " = " + (float) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.DOUBLE) {
            filter = columnRichInfo.getColumnName() + " = " + (double) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.DECIMAL) {
            filter = columnRichInfo.getColumnName() + " = " + (DecimalData) obj;
        }

        if (!filter.equals("")) {
            filterList.add(filter);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
