package admin;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ClientExample {
    public static Client newTransportClient() {
        Client result = newTransportClient("elasticsearch", "localhost");
        return result;
    }
    
    public static Client newTransportClient(String cluster, String host) {
        Settings settings = ImmutableSettings.settingsBuilder()
            .put("cluster.name", cluster)
            .put("client.transport.sniff", true)
            .build();
              
        Client result = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(host, 9300));
        
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
