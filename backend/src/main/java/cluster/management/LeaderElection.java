package cluster.management;

/**
 * Implementation of a leader election algorithm.
 * This algorithm uses apache zookeeper as a coordination service,
 * and depends on the global sequence number guaranteed by the zookeeper
 * to help elect the leader during an election.
 *
 * The service that is able to created a znode with the smallest
 * sequence number among all, declares itself as the leader.
 */
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

/**
 * To be a Watcher of the zookeeper event,
 * the class needs to implement the Watcher interface.
 */
public class LeaderElection implements Watcher {

    private static final String ELECTION_ZNODE = "/election";

    private String currentZnodeName;

    private final ZooKeeper zooKeeper;

    private final OnElectionCallback onElectionCallback;

    public LeaderElection(ZooKeeper zooKeeper, OnElectionCallback onElectionCallback) {
        this.zooKeeper = zooKeeper;
        this.onElectionCallback = onElectionCallback;
    }



    public void selfElectForLeader() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_ZNODE + "/c_";

        /*
            Each node creates a ephemeral znode on boot.
         */
        String znodeFullPath = zooKeeper.create(znodePrefix,
                new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL); //create sequential and ephemeral znodes. Crucial for leader election and re-election to work.

        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_ZNODE + "/", ""); //just the name - sequence number.

    }

    public void electLeader() throws InterruptedException, KeeperException {
        String predecessorZnodeName = "";
        Stat predecessorStat = null;

        /*
            Keep trying until I find a prev znode to hook onto
            or I become the leader. Essential of leader re-election.
         */
        while (predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_ZNODE, false); //names (without path) of the children of ELECTION_ZNODE.

            Collections.sort(children);
            String smallestChild = children.get(0);

            /*
                The node which creates the znode with smallest sequence
                number is the leader.
             */
            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader!");

                /*
                    Current node is a leader. Declare it
                    so that service registry knows.
                 */
                onElectionCallback.onElectedToBeLeader();
                return;
            } else {
                System.out.println("I ain't the leader." + smallestChild + " is the leader." );
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1; //find current node's predecessor's index.
                predecessorZnodeName = children.get(predecessorIndex);

                /*
                    Current node will watch predecessor's znode --> core of the leader re-election algorithm.
                    It might happen that by the time this line is executed and the current node is able to
                    watch prev nodes' znode, the prev node (and its ephemeral znode) might already be dead, and
                    it might happen that there is a breakage in the chain. Therefore, we do this in a loop and keep
                    trying to find a predecessor znode to hook onto or until the current node becomes the leader itself.
                 */
                predecessorStat = zooKeeper.exists(ELECTION_ZNODE + "/" + predecessorZnodeName, this);
            }
        }

        /*
            Not a leader, instead a worker. Declare that.
         */
        onElectionCallback.onWorker();


        System.out.println("Watching znode " + predecessorZnodeName);
        System.out.println();
    }


    /**
     * The process method will be called on a separate (event thread, not main) thread
     * by the zookeeper library whenever an event comes from the zookeeper server.
     * @param watchedEvent
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case NodeDeleted -> {
                try {
                    /*
                        The znode that current node was watching got deleted.
                        Fix the chain and if required re-elect the leader.
                     */
                    electLeader();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}