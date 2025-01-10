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

import java.util.HashMap;
import java.util.Map;

public class StarRocksJsonSerializer implements StarRocksISerializer {

    private static final long serialVersionUID = 1L;
    
    private final String[] fieldNames;

    private transient JsonWrapper jsonWrapper;

    public StarRocksJsonSerializer(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public void open(SerializerContext context) {
        this.jsonWrapper = context.getFastJsonWrapper();
    }

    @Override
    public String serialize(Object[] values) {
        if (values.length == 0)
            return "";

        Map<String, Object> rowMap = new HashMap<>(values.length);
        int idx = 0;
        for (String fieldName : fieldNames) {
            rowMap.put(fieldName, values[idx]);
            idx++;
        }

        return jsonWrapper.toJSONString(rowMap);
    }
    
}
