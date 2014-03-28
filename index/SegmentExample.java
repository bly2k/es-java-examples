package index;

import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.Segment;

import java.io.IOException;

public class SegmentExample {
    public static void makeSegments(Client client) {
        String index = "test";

        //delete index if exists
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists())
            client.admin().indices().prepareDelete(index).execute().actionGet();
        
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .put("refresh_interval", -1)
                .build();
        
        client.admin().indices().prepareCreate(index).setSettings(settings).execute().actionGet();
        
        client.admin().cluster().prepareHealth(index).setWaitForYellowStatus().execute().actionGet();
    
        try {
            final int PER_SEGMENT_DOCS = 10; 
            
            int docs = 0;
            for (int i = 0; i < 100; i++) {
                XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                        .field("text", "Doc " + i)
                    .endObject();
                
                client.prepareIndex(index, "doc").setSource(doc).setId(Integer.toString(i)).execute().actionGet();
                
                if (++docs >= PER_SEGMENT_DOCS) {
                    //make a searchable, non-committed segment
                    client.admin().indices().prepareRefresh(index).execute().actionGet();
                    docs = 0;
                }
            }
            
            //commit all segments
            client.admin().indices().prepareFlush(index).execute().actionGet();
            
            IndicesSegmentResponse response = client.admin().indices().prepareSegments(index).execute().actionGet();
            int i = 0;
            for (Segment s: response.getIndices().get(index).getShards().get(0).getShards()[0].getSegments()) {
                System.out.println("Segment [" + (++i) + "], Name: \"" + s.getName() + "\", Docs: " + s.getNumDocs());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
