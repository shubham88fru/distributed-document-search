import cluster.management.OnElectionCallback;
import cluster.management.ServiceRegistryAndDiscovery;
import networking.WebClient;
import networking.WebServer;
import org.apache.zookeeper.KeeperException;
import search.SearchCoordinator;
import search.SearchWorker;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {
    private final ServiceRegistryAndDiscovery workerServiceRegistry;
    private final ServiceRegistryAndDiscovery coordinatorServiceRegistry;
    private final int port;
    private WebServer webServer;

    public OnElectionAction(ServiceRegistryAndDiscovery workerServiceRegistry,
                            ServiceRegistryAndDiscovery coordinatorServiceRegistry, int port) {
        this.workerServiceRegistry = workerServiceRegistry;
        this.coordinatorServiceRegistry = coordinatorServiceRegistry;
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
            workerServiceRegistry.unregisterFromCluster();
            workerServiceRegistry.registerForUpdates();

            if (webServer != null) {
                webServer.stop();
            }

            SearchCoordinator searchCoordinator = new SearchCoordinator(workerServiceRegistry, new WebClient());
            webServer = new WebServer(port, searchCoordinator);
            webServer.startServer();

            String currentServerAddress =
                    String.format("http://%s:%d%s", InetAddress.getLocalHost().getCanonicalHostName(),
                            port, searchCoordinator.getEndpoint());
            coordinatorServiceRegistry.registerToCluster(currentServerAddress);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (UnknownHostException e) {
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
                    String.format("http://%s:%d%s", InetAddress.getLocalHost().getCanonicalHostName(),
                            port, searchWorker.getEndpoint());
            workerServiceRegistry.registerToCluster(currentServerAddress);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}