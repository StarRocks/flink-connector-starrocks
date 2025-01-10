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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.connector.flink.row.sink.StarRocksDelimiterParser;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.http.protocol.HttpRequestExecutor.DEFAULT_WAIT_FOR_CONTINUE;

public class StarRocksSinkOptions implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(StarRocksSinkOptions.class);

    private static final long serialVersionUID = 1L;
    private static final long KILO_BYTES_SCALE = 1024L;
    private static final long MEGA_BYTES_SCALE = KILO_BYTES_SCALE * KILO_BYTES_SCALE;
    private static final long GIGA_BYTES_SCALE = MEGA_BYTES_SCALE * KILO_BYTES_SCALE;

    public enum StreamLoadFormat {
        CSV, JSON;
    }

    private static final String FORMAT_KEY = "format";

    // required sink configurations
    public static final ConfigOption<String> JDBC_URL = ConfigOptions.key("jdbc-url")
            .stringType().noDefaultValue().withDescription("Url of the jdbc like: `jdbc:mysql://fe_ip1:query_port,fe_ip2:query_port...`.");
    public static final ConfigOption<List<String>> LOAD_URL = ConfigOptions.key("load-url")
            .stringType().asList().noDefaultValue().withDescription("Url of the stream load, if you you don't specify the http/https prefix, the default http. like: `fe_ip1:http_port;http://fe_ip2:http_port;https://fe_nlb`.");
    public static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name")
            .stringType().noDefaultValue().withDescription("Database name of the stream load.");
    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
            .stringType().noDefaultValue().withDescription("Table name of the stream load.");
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType().noDefaultValue().withDescription("StarRocks user name.");
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType().noDefaultValue().withDescription("StarRocks user password.");

    // optional sink configurations
    public static final ConfigOption<String> SINK_VERSION = ConfigOptions.key("sink.version")
            .stringType()
            .defaultValue(SinkFunctionFactory.SinkVersion.AUTO.name())
            .withDescription("Version of the sink");

    public static final ConfigOption<String> SINK_LABEL_PREFIX = ConfigOptions.key("sink.label-prefix")
            .stringType().noDefaultValue().withDescription("The prefix of the stream load label. Available values are within [-_A-Za-z0-9]");
    public static final ConfigOption<Integer> SINK_CONNECT_TIMEOUT = ConfigOptions.key("sink.connect.timeout-ms")
            .intType().defaultValue(30000).withDescription("Timeout in millisecond for connecting to the `load-url`.");

    public static final ConfigOption<Integer> SINK_SOCKET_TIMEOUT = ConfigOptions.key("sink.socket.timeout-ms")
            .intType().defaultValue(-1).withDescription("Timeout in milliseconds that the http client waits for data or, put differently, " +
                    "a maximum period inactivity between two consecutive data packets. The default value -1 is same as that of apache http " +
                    "client which is interpreted as undefined (system default if applicable). A timeout value of zero is interpreted as " +
                    "an infinite timeout. You can use this option to fail the stream load from the connector side if the http client does not " +
                    "receive response from StarRocks before timeout. The other option 'sink.properties.timeout' take effects on the StarRocks " +
                    "side, but the response to the client may delay in some unexpected cases. If you want to have a strict timeout from the " +
                    "connector side, you can set this option to an acceptable value.");
    public static final ConfigOption<Integer> SINK_WAIT_FOR_CONTINUE_TIMEOUT = ConfigOptions.key("sink.wait-for-continue.timeout-ms")
            .intType().defaultValue(30000).withDescription("Timeout in millisecond to wait for 100-continue response for http client.");
    public static final ConfigOption<Integer> SINK_IO_THREAD_COUNT = ConfigOptions.key("sink.io.thread-count")
            .intType().defaultValue(2).withDescription("Stream load thread count");

    public static final ConfigOption<Long> SINK_CHUNK_LIMIT = ConfigOptions.key("sink.chunk-limit")
            .longType().defaultValue(3 * GIGA_BYTES_SCALE).withDescription("Data chunk size in a http request for stream load");

    public static final ConfigOption<Long> SINK_SCAN_FREQUENCY = ConfigOptions.key("sink.scan-frequency.ms")
            .longType().defaultValue(50L).withDescription("Scan frequency in milliseconds.");

    public static final ConfigOption<String> SINK_SEMANTIC = ConfigOptions.key("sink.semantic")
            .stringType().defaultValue(StarRocksSinkSemantic.AT_LEAST_ONCE.getName()).withDescription("Fault tolerance guarantee. `at-least-once` or `exactly-once`");
    public static final ConfigOption<Long> SINK_BATCH_MAX_SIZE = ConfigOptions.key("sink.buffer-flush.max-bytes")
            .longType().defaultValue(90L * MEGA_BYTES_SCALE).withDescription("Max data bytes of the flush.");
    public static final ConfigOption<Long> SINK_BATCH_MAX_ROWS = ConfigOptions.key("sink.buffer-flush.max-rows")
            .longType().defaultValue(500000L).withDescription("Max row count of the flush.");
    public static final ConfigOption<Long> SINK_BATCH_FLUSH_INTERVAL = ConfigOptions.key("sink.buffer-flush.interval-ms")
            .longType().defaultValue(300000L).withDescription("Flush interval of the row batch in millisecond.");

    public static final ConfigOption<Boolean> SINK_AT_LEAST_ONCE_USE_TRANSACTION_LOAD = ConfigOptions.key("sink.at-least-once.use-transaction-stream-load")
            .booleanType().defaultValue(true).withDescription("Whether to use transaction stream load for at-least-once when it's available.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions.key("sink.max-retries")
            .intType().defaultValue(3).withDescription("Max flushing retry times of the row batch.");

    public static final ConfigOption<Integer> SINK_RETRY_INTERVAL = ConfigOptions.key("sink.retry.interval-ms")
            .intType().defaultValue(10000).withDescription("Interval in milliseconds between tries.");

    public static final ConfigOption<Long> SINK_BATCH_OFFER_TIMEOUT = ConfigOptions.key("sink.buffer-flush.enqueue-timeout-ms")
            .longType().defaultValue(600000L).withDescription("Offer to flushQueue timeout in millisecond.");
    public static final ConfigOption<Integer> SINK_METRIC_HISTOGRAM_WINDOW_SIZE = ConfigOptions.key("sink.metric.histogram-window-size")
            .intType().defaultValue(100).withDescription("Window size of histogram metrics.");

    public static final ConfigOption<Boolean> SINK_IGNORE_UPDATE_BEFORE = ConfigOptions.key("sink.ignore.update-before")
            .booleanType().defaultValue(true).withDescription("Whether to ignore update_before records. In general, update_before " +
                    "and update_after have the same primary key, and appear in pair, so we only need to write the update_after " +
                    "record to StarRocks, and ignore the update_before. But that hasn't always been the case, for example, if the " +
                    "user updates one row with the primary key changed in the OLTP, and Flink cdc will generate a before and after" +
                    "records, but they have the different primary keys. The connector should delete the update_before row, and " +
                    "insert the update_after row in StarRocks, and this options should be set false for this case. Note that how " +
                    "to set this options depends on the user case.");

    public static final ConfigOption<Boolean> SINK_ENABLE_EXACTLY_ONCE_LABEL_GEN = ConfigOptions.key("sink.exactly-once.enable-label-gen")
            .booleanType().defaultValue(true).withDescription("Only available when using exactly-once and sink.label-prefix is set. " +
                    "When it's true, the connector will generate label in the format '{labelPrefix}-{tableName}-{subtaskIndex}-{id}'. " +
                    "This format could be used to track lingering transactions.");

    public static final ConfigOption<Boolean> SINK_ABORT_LINGERING_TXNS = ConfigOptions.key("sink.exactly-once.enable-abort-lingering-txn")
            .booleanType().defaultValue(true).withDescription("Only available when using exactly-once and sink.label-prefix is set. " +
                    "Whether to abort lingering transactions when the job restore.");

    public static final ConfigOption<Integer> SINK_ABORT_CHECK_NUM_TXNS = ConfigOptions.key("sink.exactly-once.check-num-lingering-txn")
            .intType().defaultValue(-1).withDescription("Only available when sink.exactly-once.abort-lingering-txn is enabled. " +
                    "The number of transactions to check if they are lingering. -1 indicates that check until finding the first " +
                    "transaction that is not lingering.");

    public static final ConfigOption<Boolean> SINK_USE_NEW_SINK_API = ConfigOptions.key("sink.use.new-sink-api")
            .booleanType().defaultValue(false).withDescription("Whether to use the implementation with the unified sink api " +
                    "described in Flink FLIP-191. There is no difference for users whether to enable this flag. This is just " +
                    "for adapting some frameworks which only support new sink api, and Flink will also remove the old sink api " +
                    "in the coming 2.0. Note that it's not compatible after changing the flag, that's, you can't recover from " +
                    "the previous job after changing the flag.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    public static final ConfigOption<Boolean> SINK_WRAP_JSON_AS_ARRAY = ConfigOptions.key("sink.wrap-json-as-array")
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether wrap data as array or not.");

    // Sink semantic
    private static final Set<String> SINK_SEMANTIC_ENUMS = Arrays.stream(StarRocksSinkSemantic.values()).map(s -> s.getName()).collect(Collectors.toSet());
    // wild stream load properties' prefix
    public static final String SINK_PROPERTIES_PREFIX = "sink.properties.";

    private final ReadableConfig tableOptions;
    private final Map<String, String> streamLoadProps = new HashMap<>();
    private final Map<String, String> tableOptionsMap;
    private StarRocksSinkSemantic sinkSemantic;
    private boolean supportUpsertDelete;
    private String[] tableSchemaFieldNames;

    private boolean supportTransactionStreamLoad = false;

    private final List<StreamLoadTableProperties> tablePropertiesList = new ArrayList<>();

    public StarRocksSinkOptions(ReadableConfig options, Map<String, String> optionsMap) {
        this.tableOptions = options;
        this.tableOptionsMap = optionsMap;
        parseSinkStreamLoadProperties();
        this.validate();
    }

    public StarRocksSinkOptions addTableProperties(StreamLoadTableProperties tableProperties) {
        tablePropertiesList.add(tableProperties);
        return this;
    }

    public List<Tuple2<String, String>> getDbTables() {
        Set<Tuple2<String, String>> dbTables = new HashSet<>();
        if (getDatabaseName() != null && getTableName() != null) {
            dbTables.add(Tuple2.of(getDatabaseName(), getTableName()));
        }

        for (StreamLoadTableProperties properties : tablePropertiesList) {
            dbTables.add(Tuple2.of(properties.getDatabase(), getTableName()));
        }

        return new ArrayList<>(dbTables);
    }

    private void validate() {
        validateRequired();
        validateStreamLoadUrl();
        validateSinkSemantic();
        validateParamsRange();
    }

    public void setTableSchemaFieldNames(String[] fieldNames) {
        this.tableSchemaFieldNames = new String[fieldNames.length];
        System.arraycopy(fieldNames, 0, tableSchemaFieldNames, 0, fieldNames.length);
    }

    public String[] getTableSchemaFieldNames() {
        return tableSchemaFieldNames;
    }

    public String getJdbcUrl() {
        return tableOptions.get(JDBC_URL);
    }

    public String getDatabaseName() {
        return tableOptions.get(DATABASE_NAME);
    }

    public String getTableName() {
        return tableOptions.get(TABLE_NAME);
    }

    public String getUsername() {
        return tableOptions.get(USERNAME);
    }

    public String getPassword() {
        return tableOptions.get(PASSWORD);
    }

    public String getSinkVersion() {
        return tableOptions.get(SINK_VERSION);
    }

    public List<String> getLoadUrlList() {
        return tableOptions.getOptional(LOAD_URL).orElse(null);
    }

    public String getLabelPrefix() {
        return tableOptions.getOptional(SINK_LABEL_PREFIX).orElse(null);
    }

    public int getSinkMaxRetries() {
        return tableOptions.get(SINK_MAX_RETRIES);
    }

    public int getRetryIntervalMs() {
        return tableOptions.get(SINK_RETRY_INTERVAL);
    }

    public long getSinkMaxFlushInterval() {
        return tableOptions.get(SINK_BATCH_FLUSH_INTERVAL);
    }

    public long getSinkMaxRows() {
        return tableOptions.get(SINK_BATCH_MAX_ROWS);
    }

    public long getSinkMaxBytes() {
        return tableOptions.get(SINK_BATCH_MAX_SIZE);
    }

    public int getConnectTimeout() {
        int connectTimeout = tableOptions.get(SINK_CONNECT_TIMEOUT);
        if (connectTimeout < 100) {
            return 100;
        }
        return Math.min(connectTimeout, 60000);
    }

    public int getSocketTimeout() {
        return tableOptions.get(SINK_SOCKET_TIMEOUT);
    }

    public int getWaitForContinueTimeout() {
        int waitForContinueTimeoutMs = tableOptions.get(SINK_WAIT_FOR_CONTINUE_TIMEOUT);
        if (waitForContinueTimeoutMs < DEFAULT_WAIT_FOR_CONTINUE) {
            return DEFAULT_WAIT_FOR_CONTINUE;
        }
        return Math.min(waitForContinueTimeoutMs, 600000);
    }

    public int getIoThreadCount() {
        return tableOptions.get(SINK_IO_THREAD_COUNT);
    }

    public long getChunkLimit() {
        return tableOptions.get(SINK_CHUNK_LIMIT);
    }

    public long getScanFrequency() {
        return tableOptions.get(SINK_SCAN_FREQUENCY);
    }

    public long getSinkOfferTimeout() {
        return tableOptions.get(SINK_BATCH_OFFER_TIMEOUT);
    }

    public int getSinkHistogramWindowSize() {
        return tableOptions.get(SINK_METRIC_HISTOGRAM_WINDOW_SIZE);
    }

    public Integer getSinkParallelism() {
        return tableOptions.getOptional(SINK_PARALLELISM).orElse(null);
    }

    public boolean getIgnoreUpdateBefore() {
        return tableOptions.get(SINK_IGNORE_UPDATE_BEFORE);
    }

    public static Builder builder() {
        return new Builder();
    }

    public StarRocksSinkSemantic getSemantic() {
        return this.sinkSemantic;
    }

    public boolean getSinkAtLeastOnceUseTransactionStreamLoad() {
        return tableOptions.get(SINK_AT_LEAST_ONCE_USE_TRANSACTION_LOAD);
    }

    public Map<String, String> getSinkStreamLoadProperties() {
        return streamLoadProps;
    }

    public boolean hasColumnMappingProperty() {
        return streamLoadProps.containsKey("columns");
    }

    public StreamLoadFormat getStreamLoadFormat() {
        Map<String, String> loadProsp = getSinkStreamLoadProperties();
        String format = loadProsp.get(FORMAT_KEY);
        if (StreamLoadFormat.JSON.name().equalsIgnoreCase(format)) {
            return StreamLoadFormat.JSON;
        }
        return StreamLoadFormat.CSV;
    }

    public void enableUpsertDelete() {
        supportUpsertDelete = true;
    }

    public boolean supportUpsertDelete() {
        return supportUpsertDelete;
    }

    public void setSupportTransactionStreamLoad(boolean supportTransactionStreamLoad) {
        this.supportTransactionStreamLoad = supportTransactionStreamLoad;
    }

    public boolean isSupportTransactionStreamLoad() {
        return supportTransactionStreamLoad;
    }

    public boolean isEnableExactlyOnceLabelGen() {
        return tableOptions.get(SINK_ENABLE_EXACTLY_ONCE_LABEL_GEN);
    }

    public boolean isAbortLingeringTxns() {
        return tableOptions.get(SINK_ABORT_LINGERING_TXNS);
    }

    public int getAbortCheckNumTxns() {
        return tableOptions.get(SINK_ABORT_CHECK_NUM_TXNS);
    }

    public boolean isUseUnifiedSinkApi() {
        return tableOptions.get(SINK_USE_NEW_SINK_API);
    }

    public boolean isWrapJsonAsArray() {
        return tableOptions.get(SINK_WRAP_JSON_AS_ARRAY);
    }

    private void validateStreamLoadUrl() {
        tableOptions.getOptional(LOAD_URL).ifPresent(urlList -> {
            for (String host : urlList) {
                if (host.split(":").length < 2) {
                    throw new ValidationException(String.format(
                            "Could not parse host '%s' in option '%s'. It should follow the format 'host_name:port'.",
                            host,
                            LOAD_URL.key()));
                }
            }
        });
    }

    private void validateSinkSemantic() {
        tableOptions.getOptional(SINK_SEMANTIC).ifPresent(semantic -> {
            if (!SINK_SEMANTIC_ENUMS.contains(semantic)){
                throw new ValidationException(
                        String.format("Unsupported value '%s' for '%s'. Supported values are ['at-least-once', 'exactly-once'].",
                                semantic, SINK_SEMANTIC.key()));
            }
        });
        this.sinkSemantic = StarRocksSinkSemantic.fromName(tableOptions.get(SINK_SEMANTIC));
    }

    private void validateParamsRange() {
        tableOptions.getOptional(SINK_MAX_RETRIES).ifPresent(val -> {
            if (val < 0 || val > 1000) {
                throw new ValidationException(
                        String.format("Unsupported value '%d' for '%s'. Supported value range: [0, 1000].",
                                val, SINK_MAX_RETRIES.key()));
            }
        });
        tableOptions.getOptional(SINK_BATCH_FLUSH_INTERVAL).ifPresent(val -> {
            if (val < 1000L || val > 3600000L) {
                throw new ValidationException(
                        String.format("Unsupported value '%d' for '%s'. Supported value range: [1000, 3600000].",
                                val, SINK_BATCH_FLUSH_INTERVAL.key()));
            }
        });
        tableOptions.getOptional(SINK_BATCH_MAX_ROWS).ifPresent(val -> {
            if (val < 64000 || val > 5000000) {
                throw new ValidationException(
                        String.format("Unsupported value '%d' for '%s'. Supported value range: [64000, 5000000].",
                                val, SINK_BATCH_MAX_ROWS.key()));
            }
        });
        tableOptions.getOptional(SINK_BATCH_MAX_SIZE).ifPresent(val -> {
            if (val < 64 * MEGA_BYTES_SCALE || val > 10 * GIGA_BYTES_SCALE) {
                throw new ValidationException(
                        String.format("Unsupported value '%d' for '%s'. Supported value range: [%d, %d].",
                                val, SINK_BATCH_MAX_SIZE.key(), 64 * MEGA_BYTES_SCALE, 10 * GIGA_BYTES_SCALE));
            }
        });
        tableOptions.getOptional(SINK_BATCH_OFFER_TIMEOUT).ifPresent(val -> {
            if (val < 300000) {
                throw new ValidationException(
                        String.format("Unsupported value '%d' for '%s'. Supported value range: [300000, Long.MAX_VALUE].",
                                val, SINK_BATCH_OFFER_TIMEOUT.key()));
            }
        });
    }

    private void validateRequired() {
        ConfigOption<?>[] configOptions = new ConfigOption[]{
                USERNAME,
                PASSWORD,
                TABLE_NAME,
                DATABASE_NAME,
                JDBC_URL,
                LOAD_URL
        };
        int presentCount = 0;
        for (ConfigOption<?> configOption : configOptions) {
            if (tableOptions.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames = Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n" + String.join("\n", propertyNames));
    }

    private void parseSinkStreamLoadProperties() {
        tableOptionsMap.keySet().stream()
                .filter(key -> key.startsWith(SINK_PROPERTIES_PREFIX))
                .forEach(key -> {
                    final String value = tableOptionsMap.get(key);
                    final String subKey = key.substring((SINK_PROPERTIES_PREFIX).length()).toLowerCase();
                    streamLoadProps.put(subKey, value);
                });
    }

    /**
     * Builder for {@link StarRocksSinkOptions}.
     */
    public static final class Builder {
        private final Configuration conf;
        public Builder() {
            conf = new Configuration();
        }

        public Builder withProperty(String key, String value) {
            conf.setString(key, value);
            return this;
        }

        public StarRocksSinkOptions build() {
            return new StarRocksSinkOptions(conf, conf.toMap());
        }
    }

    public StreamLoadProperties getProperties(@Nullable StarRocksSinkTable table) {
        StarRocksSinkTable sinkTable = table;
        if (sinkTable == null) {
            sinkTable = StarRocksSinkTable.builder()
                    .sinkOptions(this)
                    .build();
        }

        StreamLoadDataFormat dataFormat;
        if (getStreamLoadFormat() == StarRocksSinkOptions.StreamLoadFormat.CSV) {
            dataFormat = new StreamLoadDataFormat.CSVFormat(StarRocksDelimiterParser
                    .parse(getSinkStreamLoadProperties().get("row_delimiter"), "\n"));
        } else if (getStreamLoadFormat() == StarRocksSinkOptions.StreamLoadFormat.JSON) {
            dataFormat = StreamLoadDataFormat.JSON;
        } else {
            throw new RuntimeException("data format are not support");
        }

        StreamLoadTableProperties.Builder defaultTablePropertiesBuilder = StreamLoadTableProperties.builder()
                .database(getDatabaseName())
                .table(getTableName())
                .streamLoadDataFormat(dataFormat)
                .chunkLimit(getChunkLimit())
                .enableUpsertDelete(supportUpsertDelete())
                .addCommonProperties(getSinkStreamLoadProperties());

        if (hasColumnMappingProperty()) {
            defaultTablePropertiesBuilder.columns(streamLoadProps.get("columns"));
        } else if (getTableSchemaFieldNames() != null) {
            // don't need to add "columns" header in following cases
            // 1. use csv format but the flink and starrocks schemas are aligned
            // 2. use json format, except that it's loading o a primary key table for StarRocks 1.x
            boolean noNeedAddColumnsHeader;
            if (dataFormat instanceof StreamLoadDataFormat.CSVFormat) {
                noNeedAddColumnsHeader = sinkTable.isFlinkAndStarRocksColumnsAligned();
            } else {
                noNeedAddColumnsHeader = !supportUpsertDelete() || sinkTable.isOpAutoProjectionInJson();
            }

            if (!noNeedAddColumnsHeader) {
                String[] columns;
                if (supportUpsertDelete()) {
                    columns = new String[getTableSchemaFieldNames().length + 1];
                    System.arraycopy(getTableSchemaFieldNames(), 0, columns, 0, getTableSchemaFieldNames().length);
                    columns[getTableSchemaFieldNames().length] = "__op";
                } else {
                    columns = getTableSchemaFieldNames();
                }

                String cols = Arrays.stream(columns)
                        .map(f -> String.format("`%s`", f.trim().replace("`", "")))
                        .collect(Collectors.joining(","));
                defaultTablePropertiesBuilder.columns(cols);
            }
        }

        Map<String, String> streamLoadProperties = new HashMap<>(getSinkStreamLoadProperties());
        // By default, using json format should enable strip_outer_array and ignore_json_size,
        // which will simplify the configurations
        if (dataFormat instanceof StreamLoadDataFormat.JSONFormat) {
            if (!streamLoadProperties.containsKey("strip_outer_array")) {
                streamLoadProperties.put("strip_outer_array", "true");
            }
            if (!streamLoadProperties.containsKey("ignore_json_size")) {
                streamLoadProperties.put("ignore_json_size", "true");
            }
        }
        StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .loadUrls(getLoadUrlList().toArray(new String[0]))
                .jdbcUrl(getJdbcUrl())
                .defaultTableProperties(defaultTablePropertiesBuilder.build())
                .cacheMaxBytes(getSinkMaxBytes())
                .connectTimeout(getConnectTimeout())
                .waitForContinueTimeoutMs(getWaitForContinueTimeout())
                .socketTimeout(getSocketTimeout())
                .ioThreadCount(getIoThreadCount())
                .scanningFrequency(getScanFrequency())
                .labelPrefix(getLabelPrefix())
                .username(getUsername())
                .password(getPassword())
                .version(sinkTable.getVersion())
                .expectDelayTime(getSinkMaxFlushInterval())
                // TODO not support retry currently
                .maxRetries(0)
                .retryIntervalInMs(getRetryIntervalMs())
                .addHeaders(streamLoadProperties);

        for (StreamLoadTableProperties tableProperties : tablePropertiesList) {
            builder.addTableProperties(tableProperties);
        }

        if (isSupportTransactionStreamLoad() &&
                (getSemantic() == StarRocksSinkSemantic.EXACTLY_ONCE
                        || getSinkAtLeastOnceUseTransactionStreamLoad())) {
            builder.enableTransaction();
            log.info("Enable transaction stream load");
        }
        return builder.build();
    }

}
