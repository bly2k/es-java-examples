package admin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;

public class ClusterHealthExample {
    public static void clusterHealth (Client client) {
        ClusterHealthResponse response = client.admin().cluster().prepareHealth().execute().actionGet();
        System.out.println(
            "Cluster Name: " + response.getClusterName() + "\n" +
            "Cluster Health: " + response.getStatus());
    }

    public static void clusterHealthAsync (Client client) {
        client.admin().cluster().prepareHealth().execute().addListener(new ActionListener<ClusterHealthResponse>() {
            @Override
            public void onResponse(ClusterHealthResponse response) {
                System.out.println(
                    "Cluster Name: " + response.getClusterName() + "\n" +
                    "Cluster Health: " + response.getStatus()
                );
            }

            @Override
            public void onFailure(Throwable e) {
            }
        });
    }
}
