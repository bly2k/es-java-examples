package admin;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.client.Client;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;

public class NodesInfoExample {
    public static void Nodes(Client client){
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.clear();
        //client .admin().cluster().nodesInfo(nodesInfoRequest)
        NodesInfoResponse response = client.admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        System.out.println("Cluster Nodes: " + response.getNodes().length);
        for (NodeInfo node: response.getNodes()) {
            System.out.println("Node: " + node.getNode().name());
        }
    }
}
