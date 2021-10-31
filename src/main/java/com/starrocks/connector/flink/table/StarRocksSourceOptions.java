package com.starrocks.connector.flink.table;

import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class StarRocksSourceOptions implements Serializable {


    private final ReadableConfig tableOptions;
    private final Map<String, String> tableOptionsMap;
    private final Map<String, String> tableSQLProps = new HashMap<>();


    // required Options
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType().noDefaultValue().withDescription("StarRocks user name.");
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType().noDefaultValue().withDescription("StarRocks user password.");

    public static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name")
            .stringType().noDefaultValue().withDescription("Database name");
    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
            .stringType().noDefaultValue().withDescription("Table name");
    
    public static final ConfigOption<String> SCAN_URL = ConfigOptions.key("scan-url")
            .stringType().noDefaultValue().withDescription("Hosts of the fe node like: `fe_ip1:http_port,fe_ip2:http_port...`.");

    // optional Options
    public static final ConfigOption<String> CONNECT_TIMEOUT_MS = ConfigOptions.key("scan.connect.timeout-ms")
            .stringType().noDefaultValue().withDescription("connect timeout");
        
    public static final ConfigOption<String> BATCH_SIZE = ConfigOptions.key("scan.params.batch-size")
            .stringType().noDefaultValue().withDescription("batch size");

    public static final ConfigOption<String> PROPERTIES = ConfigOptions.key("scan.params.properties")
            .stringType().noDefaultValue().withDescription("reserved params for use");
    
    public static final ConfigOption<String> LIMIT = ConfigOptions.key("scan.params.limit")
            .stringType().noDefaultValue().withDescription("The query limit, if specified.");


    public static final ConfigOption<String> KEEP_ALIVE_MIN = ConfigOptions.key("scan.params.keep-alive-min")
            .stringType().noDefaultValue().withDescription("max keep alive time min");
    
    public static final ConfigOption<String> QUERTY_TIMEOUT = ConfigOptions.key("scan.params.query-timeout")
            .stringType().noDefaultValue().withDescription("query timeout for a single query");

    public static final ConfigOption<String> MEM_LIMIT = ConfigOptions.key("scan.params.mem-limit")
            .stringType().noDefaultValue().withDescription("memory limit for a single query");



    // ????
    public static final ConfigOption<String> COLUMNS = ConfigOptions.key("columns")
            .stringType().noDefaultValue().withDescription("columns");
    public static final ConfigOption<String> FILTER = ConfigOptions.key("filter")
            .stringType().noDefaultValue().withDescription("filters");
    
    public static final String SOURCE_PROPERTIES_PREFIX = "scan.params.";

    public StarRocksSourceOptions(ReadableConfig options, Map<String, String> optionsMap) {
        this.tableOptions = options;
        this.tableOptionsMap = optionsMap;
        parseSourceProperties();
        this.validateRequired();
    }

    private void parseSourceProperties() {
        tableOptionsMap.keySet().stream()
                .filter(key -> key.startsWith(SOURCE_PROPERTIES_PREFIX))
                .forEach(key -> {
                    final String value = tableOptionsMap.get(key);
                    final String subKey = key.substring((SOURCE_PROPERTIES_PREFIX).length()).toLowerCase();
                    tableSQLProps.put(subKey, value);
                });
    }

    private void validateRequired() {
        ConfigOption<?>[] configOptions = new ConfigOption[]{
                USERNAME,
                PASSWORD,
                TABLE_NAME,
                DATABASE_NAME,
                SCAN_URL,
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

    // required Options
    public String getUsername() {
        return tableOptions.get(USERNAME);
    }

    public String getPassword() {
        return tableOptions.get(PASSWORD);
    }

    public String getDatabaseName() {
        return tableOptions.get(DATABASE_NAME);
    }

    public String getTableName() {
        return tableOptions.get(TABLE_NAME);
    }

    public String getScanUrl() {
        return tableOptions.get(SCAN_URL);
    }


    // optional Options
    public String getConnectTimeoutMs() { return tableOptions.get(CONNECT_TIMEOUT_MS); }

    
    public String getBatchSize() {
        return tableOptions.get(BATCH_SIZE);
    }

    public String getProperties() {
        return tableOptions.get(PROPERTIES);
    }

    public String getLimit() {
        return tableOptions.get(LIMIT);
    }

    public String getKeepAliveMin() {
        return tableOptions.get(KEEP_ALIVE_MIN);
    }

    public String getQueryTimeout() {
        return tableOptions.get(QUERTY_TIMEOUT);
    }

    public String getMemLimit() {
        return tableOptions.get(MEM_LIMIT);
    }

    // ????
    public String getColums() {
        return tableOptions.get(COLUMNS);
    }

    public String getFilter() {
        return tableOptions.get(FILTER);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link StarRocksSourceOptions}.
     */
    public static final class Builder {
        private final Configuration conf;
        public Builder() {
            conf = new Configuration();
        }

        public StarRocksSourceOptions.Builder withProperty(String key, String value) {
            conf.setString(key, value);
            return this;
        }

        public StarRocksSourceOptions build() {
            return new StarRocksSourceOptions(conf, conf.toMap());
        }
    }
}
