package index;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
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
            for (int i = 0; i < 100000; i++) {
                try {
                    XContentBuilder source = XContentFactory.jsonBuilder().startObject()
                            .field("test", "this is document " + i)
                        .endObject();
                    
                    bp.add(Requests.indexRequest(index).type("doc").source(source));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
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
        }
        finally {
            bp.close();
        }
    }
}
