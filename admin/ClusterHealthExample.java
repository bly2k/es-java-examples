package admin;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;

public class ClusterHealthExample {
    public static void clusterHealth (Client client) {
        ClusterHealthResponse response = client.admin().cluster().prepareHealth().execute().actionGet();
        System.out.println(
            "Cluster Name: " + response.getClusterName() + "\n" +
            "Cluster Health: " + response.getStatus());
    }
}
