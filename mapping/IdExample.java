package mapping;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class IdExample {
    public static void idPath(Client client) {
        String index = "test";

        //delete index if exists
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists())
            client.admin().indices().prepareDelete(index).execute().actionGet();
    
        try {
            client.admin().indices().prepareCreate(index).addMapping("doc", 
                jsonBuilder().startObject()
                    .startObject("doc")
                        .startObject("_id")
                            .field("path", "my_id")
                        .endObject()
                    .endObject()
                .endObject())
                .execute()
                .actionGet();
            
            client.admin().cluster().prepareHealth(index).setWaitForYellowStatus().execute().actionGet();
      
            client.prepareIndex(index, "doc")
                .setSource("{ \"my_id\": \"101\" }")
                .setRefresh(true)
                .execute()
                .actionGet();
              
            SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            System.out.println("Doc: " + response.getHits().hits()[0].id());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
