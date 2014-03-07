package admin;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ClientExample {
    public static Client newTransportClient() {
        Settings settings = ImmutableSettings.settingsBuilder()
            .put("cluster.name", "elasticsearch")
            .build();
              
        Client result = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        
        return result;
    }
}
