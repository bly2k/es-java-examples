package search;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class GetExample {
    public static void getDoc(Client client) {
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
            
            client.prepareIndex(index, "doc").setSource(doc).setId("1").execute().actionGet();
            
            GetResponse response = client.prepareGet(index, "doc", "1").execute().actionGet();
            System.out.println("Source: " + response.getSourceAsString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
