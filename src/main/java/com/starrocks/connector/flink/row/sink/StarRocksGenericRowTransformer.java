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

import com.starrocks.connector.flink.table.StarRocksDataType;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import java.util.Map;

public class StarRocksGenericRowTransformer<T> implements StarRocksIRowTransformer<T> {
    
    private static final long serialVersionUID = 1L;

    private StarRocksSinkRowBuilder<T> consumer;
    private String[] fieldNames;

    public StarRocksGenericRowTransformer(StarRocksSinkRowBuilder<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void setStarRocksColumns(Map<String, StarRocksDataType> columns) {

    }

    @Override
    public void setTableSchema(TableSchema ts) {
        fieldNames = ts.getFieldNames();
    }

    @Override
    public void setRuntimeContext(RuntimeContext ctx) {}

    @Override
    public Object[] transform(T record, boolean supportUpsertDelete) {
        Object[] rowData = new Object[fieldNames.length + (supportUpsertDelete ? 1 : 0)];
        consumer.accept(rowData, record);

        boolean isEmpty = true;
        for (Object obj : rowData) {
            if (obj != null) {
                isEmpty = false;
                break;
            }
        }
        if (isEmpty)
            return new Object[0];

        if (supportUpsertDelete && (record instanceof RowData)) {
            // set `__op` column
            rowData[rowData.length - 1] = StarRocksSinkOP.parse(((RowData)record).getRowKind()).ordinal();
        }
        return rowData;
    }
    
}
