# starrocks-connector-for-apache-flink

This is connector for [Apache Flink®](https://flink.apache.org/)

## Prerequisites

```xml
<dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>flink-connector-starrocks</artifactId>
    <!-- for Apache Flink® 1.15 -->
    <version>x.x.x_flink-1.15</version>
    <!-- for Apache Flink® 1.14 -->
    <version>x.x.x_flink-1.14_2.11</version>
    <version>x.x.x_flink-1.14_2.12</version>
    <!-- for Apache Flink® 1.13 -->
    <version>x.x.x_flink-1.13_2.11</version>
    <version>x.x.x_flink-1.13_2.12</version>
    <!-- for Apache Flink® 1.12 -->
    <version>x.x.x_flink-1.12_2.11</version>
    <version>x.x.x_flink-1.12_2.12</version>
    <!-- for Apache Flink® 1.11 -->
    <version>x.x.x_flink-1.11_2.11</version>
    <version>x.x.x_flink-1.11_2.12</version>
</dependency>
```

Click [HERE](https://search.maven.org/search?q=g:com.starrocks) to get the latest version.

## Apache Flink® source

```java
StarRocksSourceOptions options = StarRocksSourceOptions.builder()
    .withProperty("scan-url", "fe_ip1:8030,fe_ip2:8030,fe_ip3:8030")
    .withProperty("jdbc-url", "jdbc:mysql://fe_ip:9030")
    .withProperty("username", "root")
    .withProperty("password", "")
    .withProperty("table-name", "flink_test")
    .withProperty("database-name", "test")
    .build();
TableSchema tableSchema = TableSchema.builder()
    .field("date_1", DataTypes.DATE())
    .field("datetime_1", DataTypes.TIMESTAMP(6))
    .field("char_1", DataTypes.CHAR(20))
    .field("varchar_1", DataTypes.STRING())
    .field("boolean_1", DataTypes.BOOLEAN())
    .field("tinyint_1", DataTypes.TINYINT())
    .field("smallint_1", DataTypes.SMALLINT())
    .field("int_1", DataTypes.INT())
    .field("bigint_1", DataTypes.BIGINT())
    .field("largeint_1", DataTypes.STRING())
    .field("float_1", DataTypes.FLOAT())
    .field("double_1", DataTypes.DOUBLE())
    .field("decimal_1", DataTypes.DECIMAL(27, 9))
    .build();
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.addSource(StarRocksSource.source(options, tableSchema)).setParallelism(5).print();
env.execute("StarRocks flink source");
```

OR

```java
// create a table with `structure` and `properties`
CREATE TABLE flink_test (
    date_1 DATE,
    datetime_1 TIMESTAMP(6),
    char_1 CHAR(20),
    varchar_1 VARCHAR,
    boolean_1 BOOLEAN,
    tinyint_1 TINYINT,
    smallint_1 SMALLINT,
    int_1 INT,
    bigint_1 BIGINT,
    largeint_1 STRING,
    float_1 FLOAT,
    double_1 DOUBLE,
    decimal_1 DECIMAL(27,9)
) WITH (
   'connector'='starrocks',
   'scan-url'='fe_ip1:8030,fe_ip2:8030,fe_ip3:8030',
   'jdbc-url'='jdbc:mysql://fe_ip:9030',
   'username'='root',
   'password'='',
   'database-name'='flink_test',
   'table-name'='flink_test'
);

select date_1, smallint_1 from flink_test where char_1 <> 'A' and int_1 = -126
```

### Source options

| Option                       | Required | Default            | Type   | Description                                                                                                              |
|:-----------------------------| :------- |:-------------------| :----- |:-------------------------------------------------------------------------------------------------------------------------|
| connector                    | YES      | NONE               | String | starrocks                                                                                                                |
| scan-url                     | YES      | NONE               | String | Hosts of the fe nodes like: `fe_ip1:http_port,fe_ip2:http_port...`.                                                      |
| jdbc-url                     | YES      | NONE               | String | Hosts of the fe nodes like: `fe_ip1:query_port,fe_ip2: query_port...`.                                                   |
| username                     | YES      | NONE               | String | StarRocks user name.                                                                                                     |
| password                     | YES      | NONE               | String | StarRocks user password.                                                                                                 |
| database-name                | YES      | NONE               | String | Database name                                                                                                            |
| table-name                   | YES      | NONE               | String | Table name                                                                                                               |
| scan.connect.timeout-ms      | NO       | 1000               | String | Connect timeout                                                                                                          |
| scan.params.keep-alive-min   | NO       | 10                 | String | Max keep alive time min                                                                                                  |
| scan.params.query-timeout-s  | NO       | 600(5min)          | String | Query timeout for a single query(The value of this parameter needs to be longer than the estimated period of the source) |
| scan.params.mem-limit-byte   | NO       | 1024*1024*1024(1G) | String | Memory limit for a single query                                                                                          |
| scan.max-retries             | NO       | 1                  | String | Max request retry times.                                                                                                 |
| sink.partial.update          | NO       | false              | String | Enable of partial update.                                                                                                |
### Source metrics

| Name | Type | Description |
|  :-: | :-:  | :-:  |
| totalScannedRows | counter | successfully collected data |

### Source type mappings

| StarRocks  | Flink     |
| ---------- | --------- |
| NULL       | NULL      |
| BOOLEAN    | BOOLEAN   |
| TINYINT    | TINYINT   |
| SMALLINT   | SMALLINT  |
| INT        | INT       |
| BIGINT     | BIGINT    |
| LARGEINT   | STRING    |
| FLOAT      | FLOAT     |
| DOUBLE     | DOUBLE    |
| DATE       | DATE      |
| DATETIME   | TIMESTAMP |
| DECIMAL    | DECIMAL   |
| DECIMALV2  | DECIMAL   |
| DECIMAL32  | DECIMAL   |
| DECIMAL64  | DECIMAL   |
| DECIMAL128 | DECIMAL   |
| CHAR       | CHAR      |
| VARCHAR    | STRING    |

### Source tips

1. `exactly-once` semantic cannot be guaranteed in the case of a task failure.
2. Only SQLs without aggregation like `select {*|columns|count(1)} from {table-name} where ...` are supported.

## Flink sink

```java

// -------- sink with raw json string stream --------
fromElements(new String[]{
    "{\"score\": \"99\", \"name\": \"stephen\"}",
    "{\"score\": \"100\", \"name\": \"lebron\"}"
}).addSink(
    StarRocksSink.sink(
        // the sink options
        StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", "jdbc:mysql://ip:port,ip:port?xxxxx")
            .withProperty("load-url", "ip:port;ip:port")
            .withProperty("username", "xxx")
            .withProperty("password", "xxx")
            .withProperty("table-name", "xxx")
            .withProperty("database-name", "xxx")
            .withProperty("sink.properties.format", "json")
            .withProperty("sink.properties.strip_outer_array", "true")
            .withProperty("sink.parallelism", "1")
            .build()
    )
);


// -------- sink with stream transformation --------
class RowData {
    public int score;
    public String name;
    public RowData(int score, String name) {
        ......
    }
}
fromElements(
    new RowData[]{
        new RowData(99, "stephen"),
        new RowData(100, "lebron")
    }
).addSink(
    StarRocksSink.sink(
        // the table structure
        TableSchema.builder()
            .field("score", DataTypes.INT())
            .field("name", DataTypes.VARCHAR(20))
            .build(),
        // the sink options
        StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", "jdbc:mysql://ip:port,ip:port?xxxxx")
            .withProperty("load-url", "ip:port;ip:port")
            .withProperty("username", "xxx")
            .withProperty("password", "xxx")
            .withProperty("table-name", "xxx")
            .withProperty("database-name", "xxx")
            .withProperty("sink.properties.format", "json")
            .withProperty("sink.properties.strip_outer_array", "true")
            .withProperty("sink.parallelism", "1")
            .build(),
        // set the slots with streamRowData
        (slots, streamRowData) -> {
            slots[0] = streamRowData.score;
            slots[1] = streamRowData.name;
        }
    )
);

```

OR

```java

// create a table with `structure` and `properties`
tEnv.executeSql(
    "CREATE TABLE USER_RESULT(" +
        "name VARCHAR," +
        "score BIGINT" +
    ") WITH ( " +
        "'connector' = 'starrocks'," +
        "'jdbc-url'='jdbc:mysql://ip:port,ip:port?xxxxx'," +
        "'load-url'='ip:port;ip:port'," +
        "'database-name' = 'xxx'," +
        "'table-name' = 'xxx'," +
        "'username' = 'xxx'," +
        "'password' = 'xxx'," +
        "'sink.buffer-flush.interval-ms' = '15000'," +
        "'sink.properties.format' = 'json'," +
        "'sink.properties.strip_outer_array' = 'true'," +
        "'sink.parallelism' = '1'," +
        "'sink.max-retries' = '10'," +
    ")"
);
```

## Sink options

| Option | Required | Default | Type | Description |
|  :-:  | :-:  | :-:  | :-:  | :-:  |
| connector | YES | NONE | String |`starrocks`|
| jdbc-url | YES | NONE | String | this will be used to execute queries in starrocks. |
| load-url | YES | NONE | String | `fe_ip:http_port;fe_ip:http_port` separated with `;`, which would be used to do the batch sinking. |
| database-name | YES | NONE | String | starrocks database name |
| table-name | YES | NONE | String | starrocks table name |
| username | YES | NONE | String | starrocks connecting username |
| password | YES | NONE | String | starrocks connecting password |
| sink.semantic | NO | `at-least-once` | String | `at-least-once` or `exactly-once`(`flush at checkpoint only` and options like `sink.buffer-flush.*` won't work either). |
| sink.buffer-flush.max-bytes | NO | 94371840(90M) | String | the max batching size of the serialized data, range: `[64MB, 10GB]`. |
| sink.buffer-flush.max-rows | NO | 500000 | String | the max batching rows, range: `[64,000, 5000,000]`. |
| sink.buffer-flush.interval-ms | NO | 300000 | String | the flushing time interval, range: `[1000ms, 3600000ms]`. |
| sink.max-retries | NO | 3 | String | max retry times of the stream load request, range: `[0, 1000]`. |
| sink.parallelism | NO | NULL | String | Specify the parallelism of the sink individually. Remove it if you want to follow the global parallelism settings. |
| sink.connect.timeout-ms | NO | 1000 | String | Timeout in millisecond for connecting to the `load-url`, range: `[100, 60000]`. |
| sink.label-prefix | NO | NO | String | the prefix of the stream load label, available characters are within [-_A-Za-z0-9]. |
| sink.properties.* | NO | NONE | String | the stream load properties like `'sink.properties.columns' = 'k1, v1'`. |

## Sink metrics

| Name | Type | Description |
|  :-: | :-:  | :-:  |
| totalFlushBytes | counter | successfully flushed bytes. |
| totalFlushRows | counter | successfully flushed rows. |
| totalFlushSucceededTimes | counter | number of times that the data-batch been successfully flushed. |
| totalFlushFailedTimes | counter | number of times that the flushing been failed. |

## Sink type mappings

| Flink type | StarRocks type |
|  :-: | :-: |
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INTEGER | INTEGER |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| BINARY | INT |
| CHAR | JSON / STRING |
| VARCHAR | JSON / STRING |
| STRING | JSON / STRING |
| DATE | DATE |
| TIMESTAMP_WITHOUT_TIME_ZONE(N) | DATETIME |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE(N) | DATETIME |
| ARRAY\<T\> | ARRAY\<T\> |
| MAP\<KT,VT\> | JSON / JSON STRING |
| ROW\<arg T...\> | JSON / JSON STRING |

### Sink tips

1. `Flush` action was triggered `at-least-once` when: `cachedRows >= ${sink.buffer-flush.max-rows} || cachedBytes >= ${sink.buffer-flush.max-bytes} || idleTime >= ${sink.buffer-flush.interval-ms}`
2. `sink.buffer-flush.{max-rows|max-bytes|interval-ms}` becomes invalid when it comes with the `exactly-once` semantic.
