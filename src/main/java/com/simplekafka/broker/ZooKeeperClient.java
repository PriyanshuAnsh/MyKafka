package com.simplekafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZooKeeperClient implements Watcher {

    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }
    public interface NodeCallback {
        void onNodeChanged();
    }

    private static final int SESSION_TIMEOUT = 3000;
    private String host;
    private int port;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    private ZooKeeper zooKeeper;
    private static Logger LOGGER = Logger.getLogger(ZooKeeperClient.class.getName());



    public ZooKeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await();

        createPath("/broker");
        createPath("/topics");
        createPath("/controller");

    }

    public void createPersistentNodes(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);

        if(stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created Persistent Node: " + path);
            return;
        }
        zooKeeper.setData(path, data.getBytes(), -1);
        LOGGER.info("Updated Persistent Node: " + path);
    }

    public boolean createEphemeralNodes(String path, String data) throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists(path, false);
        if(stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Created Ephemeral Node: " + path);
            return true;
        }
        zooKeeper.setData(path, data.getBytes(), -1);
        LOGGER.info("Updated Ephemeral Node: " + path);
        return false;
    }

    public void watchChild(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if(event.getType() == Event.EventType.NodeChildrenChanged) {
                    try {
                        List<String> newChildren = zooKeeper.getChildren(path, event1 -> {
                           if(event1.getType() == Event.EventType.NodeChildrenChanged) {
                               watchChild(path, callback);
                           }
                        });
                        callback.onChildrenChanged(newChildren);

                    } catch(Exception e) {
                        LOGGER.log(Level.SEVERE,"Error processing children changed event" ,e);
                    }
                }
            });
            callback.onChildrenChanged(children);
        } catch(Exception e) {
            LOGGER.log(Level.SEVERE,"Failed to watch children for path: " + path ,e);
        }
    }

    public void watchNode(String path, NodeCallback callback) {
        try {
            zooKeeper.exists(path, event -> {
                if(event.getType() == Event.EventType.NodeDeleted ||
                    event.getType() == Event.EventType.NodeCreated ||
                    event.getType() == Event.EventType.NodeDataChanged) {
                    callback.onNodeChanged();
                }
            });
        } catch(Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }
    private void createPath(String path) {
        try {
            if("/".equals(path)) {
                return;
            }
            int lastSlashIdx = path.lastIndexOf('/');

            if(lastSlashIdx > 0) {
                String parentPath = path.substring(0, lastSlashIdx);
                createPath(parentPath);
            }

            if(zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.info("Created ZooKeeper path: " + path);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to create path: " + path, e);
        }


    }

    private String getConnectString() {
        return host + ":" + port;
    }

    @Override
    public void process(WatchedEvent event) {
        if(event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            LOGGER.info("Connected to ZooKeeper!");
        } else if(event.getState() == Event.KeeperState.Disconnected) {
            LOGGER.warning("Disconnected from ZooKeeper");
        } else if(event.getState() == Event.KeeperState.Expired) {
            LOGGER.warning("ZooKeeper session expired, reconnecting...");
            try {
                if(zooKeeper != null) {
                    zooKeeper.close();
                }
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");

            } catch(Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }

    public List<String> getChildren(String path) throws InterruptedException, KeeperException {
        try {
            return zooKeeper.getChildren(path, false);
        } catch(KeeperException.NoNodeException e) {
            return new ArrayList<>();
        }
    }

    public boolean exists(String path) throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists(path, false);
        return stat != null;
    }

    public String getData(String path) throws InterruptedException, KeeperException {
        byte[] data = zooKeeper.getData(path, false, null);
        return new String(data);
    }
}
