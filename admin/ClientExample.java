package admin;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ClientExample {
    public static Client newTransportClient() {
        Settings settings = ImmutableSettings.settingsBuilder()
            .put("cluster.name", "elasticsearch")
            .put("client.transport.sniff", true)
            .build();
              
        Client result = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        
        return result;
    }
    
    public static void connectDisconnect() {
        //connect
        Client client = newTransportClient();
        
        ClusterHealthResponse response = client.admin().cluster().prepareHealth().execute().actionGet();
        System.out.println(
            "Cluster Name: " + response.getClusterName() + "\n" +
            "Cluster Health: " + response.getStatus());
        
        //disconnect
        client.close();
    }
}
