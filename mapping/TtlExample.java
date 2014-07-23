package mapping;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TtlExample {
    public static void ttl(Client client) {
        String index = "test";

        //delete index if exists
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists())
            client.admin().indices().prepareDelete(index).execute().actionGet();
    
        try {
            client.admin().indices().prepareCreate(index).addMapping("doc", 
                jsonBuilder().startObject()
                    .startObject("doc")
                        .startObject("_ttl")
                            .field("enabled", true)
                        .endObject()
                    .endObject()
                .endObject())
                .execute()
                .actionGet();
            
            client.admin().cluster().prepareHealth(index).setWaitForYellowStatus().execute().actionGet();
      
            client.prepareIndex(index, "doc")
                .setSource("{ \"foo\": \"bar\" }")
                .setTTL(1000 * 60 /*seconds*/)
                .setRefresh(true)
                .execute()
                .actionGet();
              
            SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            System.out.println("Hits: " + response.getHits().getTotalHits());
            
            //wait for ttl cleanup
            System.out.println("Waiting for ttl cleanup...");
            Thread.sleep(1000 * 60 * 2 /*minutes*/);
              
            response = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            System.out.println("Hits: " + response.getHits().getTotalHits());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
