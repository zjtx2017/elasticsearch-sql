package io.github.iamazy.elasticsearch.dsl.sql;

import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import io.github.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

/**
 * @author iamazy
 * @date 2019/2/20
 * @descrition
 **/
public class SqlParserWhereConditionTest {

    public static void testParseFromMethodSource() {
        String sql = "select _id,`@timestamp`,OS,reqUri from `micro-service-*` " +
                " where `@timestamp` between '2019-05-21' and '2019-06-01' " +
                " and reqUri is not null order by `@timestamp` desc" ;
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
        RestHighLevelClient client = initHighClient();
        try {
            SearchResponse searchResponse = parseResult.toResponse(client, RequestOptions.DEFAULT);
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            for (SearchHit searchHit : searchHits) {
                System.out.println("--------" + searchHit.getSourceAsMap());
            }
        } catch (IOException e) {
            System.out.println("查询过程中出错");
        }
        try {
            client.close();
        } catch (IOException e) {
            System.out.println("客户端关闭异常");
        }
    }

    public static void main(String[] args) {
        testParseFromMethodSource();
    }

    private static RestHighLevelClient initHighClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("172.16.10.216", 9200, "http")));
        return client;
    }

}
