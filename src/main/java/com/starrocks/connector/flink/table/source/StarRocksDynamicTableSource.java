package com.starrocks.connector.flink.table.source;

import org.apache.flink.table.api.DataTypes;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.starrocks.connector.flink.table.source.struct.PushDownHolder;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;

public class StarRocksDynamicTableSource implements ScanTableSource, SupportsLimitPushDown, SupportsFilterPushDown, SupportsProjectionPushDown {

    private final TableSchema flinkSchema;
    private final StarRocksSourceOptions options;
    private final PushDownHolder pushDownHolder;

    public StarRocksDynamicTableSource(StarRocksSourceOptions options, TableSchema schema, PushDownHolder pushDownHolder) {

        this.options = options;
        this.flinkSchema = schema;
        this.pushDownHolder = pushDownHolder;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        StarRocksDynamicSourceFunction sourceFunction = new StarRocksDynamicSourceFunction(
            options, flinkSchema, 
            this.pushDownHolder.getFilter(), 
            this.pushDownHolder.getLimit(), 
            this.pushDownHolder.getSelectColumns(), 
            this.pushDownHolder.getColumns(), 
            this.pushDownHolder.getQueryType());
        return SourceFunctionProvider.of(sourceFunction, true);
    }

    @Override
    public DynamicTableSource copy() {
        return new StarRocksDynamicTableSource(this.options, this.flinkSchema, this.pushDownHolder);
    }

    @Override
    public String asSummaryString() {
        return "StarRocks Table Source";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        // if columns = "*", this func will not be called, so 'selectColumns' will be null
        int[] curProjectedFields = Arrays.stream(projectedFields).mapToInt(value -> value[0]).toArray();
        if (curProjectedFields.length == 0 ) {
            this.pushDownHolder.setQueryType(StarRocksSourceQueryType.QueryCount);
            return;
        }
        this.pushDownHolder.setQueryType(StarRocksSourceQueryType.QuerySomeColumns);

        ArrayList<String> columnList = new ArrayList<>();
        ArrayList<SelectColumn> selectColumns = new ArrayList<SelectColumn>(); 
        for (int index : curProjectedFields) {
            String columnName = flinkSchema.getFieldName(index).get();
            columnList.add(columnName);
            selectColumns.add(new SelectColumn(columnName, index));
        }
        String columns = String.join(", ", columnList);
        this.pushDownHolder.setColumns(columns);
        this.pushDownHolder.setSelectColumns(selectColumns.toArray(new SelectColumn[selectColumns.size()]));
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filtersExpressions) {
        List<String> filters = new ArrayList<>();
        StarRocksExpressionExtractor extractor = new StarRocksExpressionExtractor();
        for (ResolvedExpression expression : filtersExpressions) {
            if (expression.getOutputDataType().equals(DataTypes.BOOLEAN()) && expression.getChildren().size() == 0) {
                filters.add(expression.accept(extractor) + " = true");
                continue;
            }
            String str = expression.accept(extractor);
            if (str == null) {
                continue;
            }
            filters.add(str);
        }
        Optional<String> filter = Optional.of(String.join(" and ", filters));
        this.pushDownHolder.setFilter(filter.get());
        return Result.of(filtersExpressions, new ArrayList<>());
    }

    @Override
    public void applyLimit(long limit) {
        this.pushDownHolder.setLimit(limit);
    }
}
