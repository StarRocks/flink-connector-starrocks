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

import com.starrocks.connector.flink.table.source.struct.PushDownHolder;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;


public final class StarRocksDynamicTableSourceFactory implements DynamicTableSourceFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return createDynamicTableSource(context, true);
    }

    public DynamicTableSource createDynamicTableSource(Context context, boolean needValidate) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        if (needValidate) {
            helper.validateExcept(StarRocksSourceOptions.SOURCE_PROPERTIES_PREFIX);
        }
        ReadableConfig options = helper.getOptions();
        // validate some special properties
        StarRocksSourceOptions sourceOptions = new StarRocksSourceOptions(options, context.getCatalogTable().getOptions());
        TableSchema flinkSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        PushDownHolder pushDownHolder = new PushDownHolder();
        return new StarRocksDynamicTableSource(sourceOptions, flinkSchema, pushDownHolder);
    }

    @Override
    public String factoryIdentifier() {
        return "starrocks";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(StarRocksSourceOptions.USERNAME);
        options.add(StarRocksSourceOptions.PASSWORD);
        options.add(StarRocksSourceOptions.TABLE_NAME);
        options.add(StarRocksSourceOptions.DATABASE_NAME);
        options.add(StarRocksSourceOptions.SCAN_URL);
        options.add(StarRocksSourceOptions.JDBC_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(StarRocksSourceOptions.SCAN_CONNECT_TIMEOUT);
        options.add(StarRocksSourceOptions.SCAN_BATCH_ROWS);
        options.add(StarRocksSourceOptions.SCAN_PROPERTIES);
        // options.add(StarRocksSourceOptions.SCAN_LIMIT);
        options.add(StarRocksSourceOptions.SCAN_KEEP_ALIVE_MIN);
        options.add(StarRocksSourceOptions.SCAN_QUERTY_TIMEOUT_S);
        options.add(StarRocksSourceOptions.SCAN_MEM_LIMIT);
        options.add(StarRocksSourceOptions.SCAN_MAX_RETRIES);
        options.add(StarRocksSourceOptions.SCAN_BE_HOST_MAPPING_LIST);
        options.add(StarRocksSourceOptions.LOOKUP_CACHE_TTL_MS);
        options.add(StarRocksSourceOptions.LOOKUP_CACHE_MAX_ROWS);
        options.add(StarRocksSourceOptions.LOOKUP_MAX_RETRIES);
        options.add(StarRocksSourceOptions.LOOKUP_CACHE_TYPE);
        return options;
    }
}
