package index;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class IndexExample {
    public static void indexDoc(Client client) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
