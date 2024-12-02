import cluster.management.OnElectionCallback;
import cluster.management.ServiceRegistryAndDiscovery;
import networking.WebServer;
import org.apache.zookeeper.KeeperException;
import search.SearchWorker;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {
    private final ServiceRegistryAndDiscovery serviceRegistryAndDiscovery;
    private final int port;
    private WebServer webServer;

    public OnElectionAction(ServiceRegistryAndDiscovery serviceRegistryAndDiscovery, int port) {
        this.serviceRegistryAndDiscovery = serviceRegistryAndDiscovery;
        this.port = port;
    }

    @Override
    public void onElectedToBeLeader() {
        try {
            /*
                If the node just joined the cluster, then the below method
                call won't do anything. However, this method call becomes
                important in the case when current node was a worker first
                and is not selected as a leader. It needs to deregister first, in that case.
             */
            serviceRegistryAndDiscovery.unregisterFromCluster();
            serviceRegistryAndDiscovery.registerForUpdates();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onWorker() {
        SearchWorker searchWorker = new SearchWorker();
        webServer = new WebServer(port, searchWorker);
        webServer.startServer();
        try {
            String currentServerAddress =
                    String.format("http://%s:%d%s", InetAddress.getLocalHost().getCanonicalHostName(), port, searchWorker.getEndpoint());
            serviceRegistryAndDiscovery.registerToCluster(currentServerAddress);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}