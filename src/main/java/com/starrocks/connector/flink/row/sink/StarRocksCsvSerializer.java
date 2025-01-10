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

package com.starrocks.connector.flink.row.sink;

import com.starrocks.connector.flink.tools.JsonWrapper;

import java.util.List;
import java.util.Map;

public class StarRocksCsvSerializer implements StarRocksISerializer {

    private static final long serialVersionUID = 1L;

    private final String columnSeparator;

    private transient JsonWrapper jsonWrapper;

    public StarRocksCsvSerializer(String sp) {
        this.columnSeparator = StarRocksDelimiterParser.parse(sp, "\t");
    }

    @Override
    public void open(SerializerContext context) {
        this.jsonWrapper = context.getFastJsonWrapper();
    }

    @Override
    public String serialize(Object[] values) {
        if (values.length == 0)
            return "";

        StringBuilder sb = new StringBuilder();
        int idx = 0;
        for (Object val : values) {
            sb.append(null == val ? "\\N" : ((val instanceof Map || val instanceof List) ? jsonWrapper.toJSONString(val) : val));
            if (idx++ < values.length - 1) {
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }
}
