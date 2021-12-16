package com.starrocks.connector.flink.table.source;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;

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

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

public class StarRocksDynamicLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicLookupFunction.class);
    
    private final ColunmRichInfo[] filterRichInfos;
    private final StarRocksSourceOptions sourceOptions;
    private final ArrayList<String> filterList;
    private QueryInfo queryInfo;
    private final SelectColumn[] selectColumns;
    private final List<ColunmRichInfo> columnRichInfos;
    private List<StarRocksSourceDataReader> dataReaderList;

    private transient Cache<Row, List<RowData>> cache;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    public StarRocksDynamicLookupFunction(StarRocksSourceOptions sourceOptions, 
                                          ColunmRichInfo[] filterRichInfos, 
                                          List<ColunmRichInfo> columnRichInfos,
                                          SelectColumn[] selectColumns
                                          ) {
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
        this.cache =
                    cacheMaxSize == -1 || cacheExpireMs == -1
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
        String SQL = "select * from " + sourceOptions.getDatabaseName() + "." + sourceOptions.getTableName() + " where " + filter;
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
            this.dataReaderList.forEach(dataReader -> {
                while (dataReader.hasNext()) {
                    RowData row = dataReader.getNext();
                    collect(row);
                }
            });
        } else {
            ArrayList<RowData> rows = new ArrayList<>();
            this.dataReaderList.forEach(dataReader -> {
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

    private void getFieldValue(Object obj, ColunmRichInfo colunmRichInfo) {

        LogicalTypeRoot flinkTypeRoot = colunmRichInfo.getDataType().getLogicalType().getTypeRoot();
        String filter = "";

        if (flinkTypeRoot == LogicalTypeRoot.DATE) {
            Calendar c = Calendar.getInstance();
            c.setTime(new Date(0L));
            c.add(Calendar.DATE, (int)obj);
            Date d = c.getTime();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            filter = colunmRichInfo.getColumnName() + " = '" + sdf.format(d).toString() + "'";
        }
        if (flinkTypeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE ||
            flinkTypeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE || 
            flinkTypeRoot == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE) {
            
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            String strDateTime = dtf.format(((TimestampData)obj).toLocalDateTime());
            filter = colunmRichInfo.getColumnName() + " = '" + strDateTime + "'";
        }
        if (flinkTypeRoot == LogicalTypeRoot.CHAR ||
            flinkTypeRoot == LogicalTypeRoot.VARCHAR) {

            filter = colunmRichInfo.getColumnName() + " = '" + ((BinaryStringData)obj).toString() + "'";
        }
        if (flinkTypeRoot == LogicalTypeRoot.BOOLEAN) {
            filter = colunmRichInfo.getColumnName() + " = " + (boolean) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.TINYINT) {
            filter = colunmRichInfo.getColumnName() + " = " + (byte) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.SMALLINT) {
            filter = colunmRichInfo.getColumnName() + " = " + (short) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.INTEGER) {
            filter = colunmRichInfo.getColumnName() + " = " + (int) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.BIGINT) {
            filter = colunmRichInfo.getColumnName() + " = " + (long) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.FLOAT) {
            filter = colunmRichInfo.getColumnName() + " = " + (float) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.DOUBLE) {
            filter = colunmRichInfo.getColumnName() + " = " + (double) obj;
        }
        if (flinkTypeRoot == LogicalTypeRoot.DECIMAL) {
            filter = colunmRichInfo.getColumnName() + " = " + (DecimalData) obj;
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
