package cluster.management;

/*
    On every leader election, one of these methods
    will be called by the nodes, to declare if
    its a worker or a leader.

 */
public interface OnElectionCallback {
    void onElectedToBeLeader();
    void onWorker();
}