package index;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

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
    
    //note: requires the attachment plugin installed
    public static void indexPdf(Client client) {
        String index = "test";
        String fileName = "/esdata/pdf/fn6742.pdf";

        //delete index if exists
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists())
            client.admin().indices().prepareDelete(index).execute().actionGet();

        try {
            client.admin().indices().prepareCreate(index).addMapping("doc", 
                jsonBuilder().startObject()
                    .field("properties").startObject()
                    .field("file").startObject()
                        .field("type", "attachment")
                        .field("fields").startObject()
                            .field("file").startObject()
                                .field("store", "yes")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject())
                .execute()
                .actionGet();
            
            client.admin().cluster().prepareHealth(index).setWaitForYellowStatus().execute().actionGet();
            
            client.prepareIndex(index, "doc", "1")
                .setSource(XContentFactory.jsonBuilder().startObject()
                    .field("text", fileName)
                    .field("file").startObject()
                        .field("content", Base64.encodeFromFile(fileName))
                        .field("_indexed_chars", -1)
                    .endObject()
                .endObject()
            ).setRefresh(true).execute().actionGet();
            
            //get it back and show text content
            SearchResponse response = client.prepareSearch(index)
                .setQuery(QueryBuilders.matchAllQuery())
                .addField("file")
                .execute()
                .actionGet();
            
            SearchHit doc = response.getHits().hits()[0];
            
            System.out.println("Content: " + doc.getFields().get("file").value().toString());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
