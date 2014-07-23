package search;

import com.google.common.collect.Maps;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.Map;

public class TemplateExample {
    public static void rawTemplate(Client client) {
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
            String q = "{\"template\": {\"query\": {\"match_all\": {}}}}";

            SearchResponse response = client.prepareSearch(index).setQuery(q).execute().actionGet();

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

    public static void sourceTemplate(Client client) {
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
            String t = "{\"template\": { \"query\": {\"match\": { \"text\": \"{{text}}\"}}}, \"params\": { \"text\": \"hello\"}}";
            BytesReference bytesRef = new BytesArray(t);

            SearchResponse response = client.prepareSearch(index).setTemplateSource(bytesRef).execute().actionGet();

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
