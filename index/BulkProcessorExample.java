package index;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class BulkProcessorExample {
    public static void bulkIndexBySize(Client client) {
        String index = "test";
        
        //delete index if exists
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists())
            client.admin().indices().prepareDelete(index).execute().actionGet();
    
        final BulkProcessor bp = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }
            
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("Bulk execution failed ["+  executionId + "].\n" +
                    failure.toString());
            }
            
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("Bulk execution completed ["+  executionId + "].\n" +
                    "Took (ms): " + response.getTookInMillis() + "\n" +
                    "Failures: " + response.hasFailures() + "\n" + 
                    "Count: " + response.getItems().length);
            }
        })
        .setConcurrentRequests(4)
        .setBulkActions(-1)
        .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
        .build();
        
        try {
            Settings settings = ImmutableSettings.settingsBuilder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
            
            client.admin().indices().prepareCreate(index).setSettings(settings).execute().actionGet();
            
            client.admin().cluster().prepareHealth(index).setWaitForYellowStatus().execute().actionGet();
        
            for (int i = 0; i < 10000; i++) {
                try {
                    XContentBuilder source = XContentFactory.jsonBuilder().startObject()
                            .field("test", "this is document " + i)
                        .endObject();
                    
                    bp.add(Requests.indexRequest(index).type("doc").source(source));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
            settings = ImmutableSettings.settingsBuilder()
                .put("refresh_interval", "1s")
                .build();
            
            client.admin().indices().prepareUpdateSettings(index).setSettings(settings).execute().actionGet();
        }
        finally {
            bp.close();
        }
    }

    public static void bulkIndexByActions(Client client) {
        String index = "test";
        
        //delete index if exists
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists())
            client.admin().indices().prepareDelete(index).execute().actionGet();
    
        final BulkProcessor bp = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }
            
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("Bulk execution failed ["+  executionId + "].\n" +
                    failure.toString());
            }
            
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("Bulk execution completed ["+  executionId + "].\n" +
                    "Took (ms): " + response.getTookInMillis() + "\n" +
                    "Failures: " + response.hasFailures() + "\n" + 
                    "Count: " + response.getItems().length);
            }
        })
        .setConcurrentRequests(4)
        .setBulkActions(1000)
        .setBulkSize(new ByteSizeValue(-1))
        .build();
        
        try {
            Settings settings = ImmutableSettings.settingsBuilder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
                
            client.admin().indices().prepareCreate(index).setSettings(settings).execute().actionGet();
            
            client.admin().cluster().prepareHealth(index).setWaitForYellowStatus().execute().actionGet();
        
            for (int i = 0; i < 10000; i++) {
                try {
                    XContentBuilder source = XContentFactory.jsonBuilder().startObject()
                            .field("test", "this is document " + i)
                        .endObject();
                    
                    bp.add(Requests.indexRequest(index).type("doc").source(source));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
            settings = ImmutableSettings.settingsBuilder()
                .put("refresh_interval", 1)
                .build();
            
            client.admin().indices().prepareUpdateSettings(index).setSettings(settings).execute().actionGet();
        }
        finally {
            bp.close();
        }
    }
}
