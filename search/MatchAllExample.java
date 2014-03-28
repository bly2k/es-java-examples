package search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

public class MatchAllExample {
    public static void matchAll(Client client) {
        String index = "test";

        //delete index if exists
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists())
            client.admin().indices().prepareDelete(index).execute().actionGet();
    
        try {
            //{
            //  "text": "Hello World"
            //}
            XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                    .field("text", "Hello World")
                .endObject();
            client.prepareIndex(index, "doc").setSource(doc).setId("1").setRefresh(true).execute().actionGet();
            
            //search
            QueryBuilder q = QueryBuilders.matchAllQuery();
            
            SearchResponse response = client.prepareSearch(index).setQuery(q).execute().actionGet();
            System.out.println(
                "Hits: " + response.getHits().getTotalHits() + "\n" +
                "Time (ms): " + response.getTookInMillis());

            int i = 0;
            for (SearchHit hit: response.getHits().hits()) {
                System.out.println(
                    "Hit " + (++i) + ": " +
                    "Source: " + hit.getSourceAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
