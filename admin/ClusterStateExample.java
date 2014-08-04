package admin;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;

public class ClusterStateExample {
    public static void indexList(Client client) {
        try {
            ClusterStateResponse response = client.admin().cluster().prepareState().execute().get();
            System.out.println("Index list:");
            for (ObjectCursor<String> cursor: response.getState().metaData().indices().keys()) {
                String index = cursor.value;
                System.out.println(index);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
