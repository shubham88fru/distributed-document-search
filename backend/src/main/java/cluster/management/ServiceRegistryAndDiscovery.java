package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistryAndDiscovery implements Watcher {
    private static final String REGISTRY_ZNODE = "/service_registry";
    private final ZooKeeper zooKeeper;

    private String currentZnode = null;

    private List<String> allServiceAddresses = null;

    public ServiceRegistryAndDiscovery(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        /*
            During boot, each node will create a ephemeral znode under the
            service registry's znode.
         */
        this.currentZnode = zooKeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes()
                ,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL); //ephemeral znode.

        System.out.println("Registered to the service registry.");
    }

    public void registerForUpdates() {
        try {
            updateAddresses();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws InterruptedException, KeeperException {
        if (allServiceAddresses == null) {
            updateAddresses();
        }

        return this.allServiceAddresses;
    }

    /*
        If a nodes is willing to leave the cluster.
        Or if say a node becomes the leader, it would want to deregister itself.
     */
    public void unregisterFromCluster() throws InterruptedException, KeeperException {
        if (currentZnode != null && (zooKeeper.exists(currentZnode, false) != null)) {
            zooKeeper.delete(currentZnode, -1);
        }
    }

    private void createServiceRegistryZnode() {
        try {
            /*
                When two nodes simultaneously execute the below `if` check, this might appear to
                be a race condition because both nodes may see that the znode is not present and
                may go inside the `if` to proceed with creation of znode. However, zookeeper internally
                handles such a situation where it will allow only one node to create a particular path.
                So, we don't have to do any extra handling for that situation.
             */
            if (zooKeeper.exists(REGISTRY_ZNODE, false) == null) { //if znode not already created.
                zooKeeper.create(REGISTRY_ZNODE, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); //persistent znode.
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /*
        this callback will be invoked when there's a change in the cluster.
        i.e. some node dropped/joined etc. So that other interested nodes
        can pull the latest address of the other nodes.
     */
    private synchronized void updateAddresses() throws InterruptedException, KeeperException {
        List<String> workerZnodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);

        List<String> addresses = new ArrayList<>(workerZnodes.size());

        for (String workerZnode: workerZnodes) {
            String workerZnodeFullPath = REGISTRY_ZNODE + "/" + workerZnode;

            /*
                If between getting the children znodes (above) and trying to
                get a znodes's data (below), the znode dies, then we'll just
                ignore and move to next.
             */
            Stat stat = zooKeeper.exists(workerZnodeFullPath, false);
            if (stat == null) continue;

            //but, if the znode does exists, then we do our thing.
            byte[] addressBytes = zooKeeper.getData(workerZnodeFullPath, false, stat);
            String address = new String(addressBytes);
            addresses.add(address);
        }

        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("Node addresses in the cluster are: " + this.allServiceAddresses);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            updateAddresses();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}