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

import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.connector.flink.row.sink.StarRocksDelimiterParser;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
    public static final ConfigOption<String > SINK_VERSION = ConfigOptions.key("sink.version")
            .stringType()
            .defaultValue(SinkFunctionFactory.SinkVersion.AUTO.name())
            .withDescription("Version of the sink");

    public static final ConfigOption<String> SINK_LABEL_PREFIX = ConfigOptions.key("sink.label-prefix")
            .stringType().noDefaultValue().withDescription("The prefix of the stream load label. Available values are within [-_A-Za-z0-9]");
    public static final ConfigOption<Integer> SINK_CONNECT_TIMEOUT = ConfigOptions.key("sink.connect.timeout-ms")
            .intType().defaultValue(1000).withDescription("Timeout in millisecond for connecting to the `load-url`.");
    public static final ConfigOption<Integer> SINK_WAIT_FOR_CONTINUE_TIMEOUT = ConfigOptions.key("sink.wait-for-continue.timeout-ms")
            .intType().defaultValue(10000).withDescription("Timeout in millisecond to wait for 100-continue response for http client.");
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
    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions.key("sink.max-retries")
            .intType().defaultValue(3).withDescription("Max flushing retry times of the row batch.");
    public static final ConfigOption<Long> SINK_BATCH_OFFER_TIMEOUT = ConfigOptions.key("sink.buffer-flush.enqueue-timeout-ms")
            .longType().defaultValue(600000L).withDescription("Offer to flushQueue timeout in millisecond.");
    public static final ConfigOption<Integer> SINK_METRIC_HISTOGRAM_WINDOW_SIZE = ConfigOptions.key("sink.metric.histogram-window-size")
            .intType().defaultValue(100).withDescription("Window size of histogram metrics.");

    public static final ConfigOption<Boolean> SINK_PARTIAL_UPDATE = ConfigOptions.key("sink.partial.update")
        .booleanType().defaultValue(false).withDescription("Enable of partial update.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

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

    public int getWaitForContinueTimeout() {
        int waitForContinueTimeoutMs = tableOptions.get(SINK_WAIT_FOR_CONTINUE_TIMEOUT);
        if (waitForContinueTimeoutMs < DEFAULT_WAIT_FOR_CONTINUE) {
            return DEFAULT_WAIT_FOR_CONTINUE;
        }
        return Math.min(waitForContinueTimeoutMs, 60000);
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

    public boolean getSinkPartialUpdate() {
        return tableOptions.get(SINK_PARTIAL_UPDATE);
    }

    public Integer getSinkParallelism() {
        return tableOptions.getOptional(SINK_PARALLELISM).orElse(null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public StarRocksSinkSemantic getSemantic() {
        return this.sinkSemantic;
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

    public StreamLoadProperties getProperties() {
        StarRocksSinkTable sinkTable = StarRocksSinkTable.builder()
                .sinkOptions(this)
                .build();

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
                .enableUpsertDelete(supportUpsertDelete());

        if (hasColumnMappingProperty()) {
            defaultTablePropertiesBuilder.columns(streamLoadProps.get("columns"));
        } else if (getTableSchemaFieldNames() != null) {
            if (dataFormat instanceof StreamLoadDataFormat.CSVFormat
                    || (!sinkTable.isOpAutoProjectionInJson() && supportUpsertDelete())) {

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

        StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .loadUrls(getLoadUrlList().toArray(new String[0]))
                .jdbcUrl(getJdbcUrl())
                .defaultTableProperties(defaultTablePropertiesBuilder.build())
                .cacheMaxBytes(getSinkMaxBytes())
                .connectTimeout(getConnectTimeout())
                .waitForContinueTimeoutMs(getWaitForContinueTimeout())
                .ioThreadCount(getIoThreadCount())
                .scanningFrequency(getScanFrequency())
                .labelPrefix(getLabelPrefix())
                .username(getUsername())
                .password(getPassword())
                .version(sinkTable.getVersion())
                .expectDelayTime(getSinkMaxFlushInterval())
                .addHeaders(getSinkStreamLoadProperties());

        for (StreamLoadTableProperties tableProperties : tablePropertiesList) {
            builder.addTableProperties(tableProperties);
        }

        if (isSupportTransactionStreamLoad()) {
            builder.enableTransaction();
            log.info("Enable transaction stream load");
        }
        return builder.build();
    }

}
