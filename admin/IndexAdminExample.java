package admin;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class IndexAdminExample {
    public static void createIndex (Client client) {
        String index = "test";

        //delete index if exists
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists())
            client.admin().indices().prepareDelete(index).execute().actionGet();
        
        Settings settings = ImmutableSettings.settingsBuilder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        
        CreateIndexResponse response = client.admin().indices().prepareCreate(index).setSettings(settings).execute().actionGet();
        
        System.out.println("Index created: " + response.isAcknowledged());
    }

    public static void createIndexWithSettingsMappings (Client client) {
        String index = "test";

        //delete index if exists
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists())
            client.admin().indices().prepareDelete(index).execute().actionGet();
        
        Settings settings = ImmutableSettings.settingsBuilder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        
        try {
            XContentBuilder type1 = XContentFactory.jsonBuilder().startObject()
                    .startObject("properties")
                        .startObject("name")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                    .endObject()
                .endObject();
            
            XContentBuilder type2 = XContentFactory.jsonBuilder().startObject()
                    .startObject("properties")
                        .startObject("product_name")
                            .field("type", "string")
                            .field("analyzer", "keyword")
                        .endObject()
                    .endObject()
                .endObject();
            
            CreateIndexResponse response = client.admin().indices().prepareCreate(index).setSettings(settings)
                .addMapping("type1", type1)
                .addMapping("type2", type2)
                .execute().actionGet();
            
            System.out.println("Index created: " + response.isAcknowledged());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createIndexFullMapping (Client client) {
        String index = "test";

        //delete index if exists
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists())
            client.admin().indices().prepareDelete(index).execute().actionGet();
        
        try {
            XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                    .startObject("settings")
                        .startObject("index")
                            .field("number_of_shards", 1)
                            .field("number_of_replicas", 0)
                            .startObject("analysis")
                                .startObject("analyzer")
                                    .startObject("lowercase_keyword")
                                        .field("tokenizer", "keyword")
                                        .array("filter", "lowercase")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("mappings")
                        .startObject("type1")
                            .startObject("properties")
                                .startObject("name")
                                    .field("type", "string")
                                    .field("index", "not_analyzed")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
            
            CreateIndexResponse response = client.admin().indices().prepareCreate(index)
                .setSource(mapping)
                .execute().actionGet();
            
            System.out.println("Index created: " + response.isAcknowledged());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
