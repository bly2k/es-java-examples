package admin;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import java.util.Map;

public class NodesInfoExample {
    public static void Nodes(Client client){
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.clear();
        NodesInfoResponse response = client.admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        System.out.println("Cluster Nodes: " + response.getNodes().length);
        for (NodeInfo node: response.getNodes()) {
            System.out.println("Node: " + node.getNode().name());
        }
    }

    public static void Indices(Client client) {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.clear();
        IndicesStatsResponse response = client.admin().indices().stats(indicesStatsRequest).actionGet();
        System.out.println("Cluster Indexes: " + response.getIndices().size());
        for (Map.Entry<String, IndexStats> i: response.getIndices().entrySet()) {
            System.out.println("Index: " + i.getKey());
        }
    }
}
