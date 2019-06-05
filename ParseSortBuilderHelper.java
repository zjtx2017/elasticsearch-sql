package io.github.iamazy.elasticsearch.dsl.sql.parser.sql.sort;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import io.github.iamazy.elasticsearch.dsl.sql.enums.QueryFieldType;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;


public class ParseSortBuilderHelper {

    public static boolean isFieldExpr(SQLExpr expr) {
        return expr instanceof SQLPropertyExpr || expr instanceof SQLIdentifierExpr;
    }

    public static boolean isMethodInvokeExpr(SQLExpr expr) {
        return expr instanceof SQLMethodInvokeExpr;
    }

    public static SortBuilder parseBasedOnFieldSortBuilder(ElasticSqlQueryField sortField, ConditionSortBuilder sortBuilder) {
        SortBuilder rtnSortBuilder = null;
        String fieldFullName = sortField.getQueryFieldFullName().replace("`","");
        if (sortField.getQueryFieldType() == QueryFieldType.RootDocField || sortField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            rtnSortBuilder = sortBuilder.buildSort(fieldFullName);
        }

        if (sortField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            FieldSortBuilder originalSort = sortBuilder.buildSort(fieldFullName);
            if(sortField.getNestedDocContextPath().size()==1) {
                originalSort.setNestedSort(new NestedSortBuilder(sortField.getNestedDocContextPath().get(0)));
            }else if(sortField.getNestedDocContextPath().size()==2){
                originalSort.setNestedSort(new NestedSortBuilder(sortField.getNestedDocContextPath().get(0)).setNestedSort(new NestedSortBuilder(sortField.getNestedDocContextPath().get(1))));
            }
            rtnSortBuilder = originalSort;
        }

        if (rtnSortBuilder == null) {
            throw new ElasticSql2DslException(String.format("[syntax error] sort condition field can not support type[%s]", sortField.getQueryFieldType()));
        }

        return rtnSortBuilder;
    }
}
