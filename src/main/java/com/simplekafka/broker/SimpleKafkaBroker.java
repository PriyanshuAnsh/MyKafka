package com.simplekafka.broker;

import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleKafkaBroker {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());
    private static final String DATA_DIR = "data";

    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final ConcurrentHashMap<String, List<Partition>> topics;
    private final AtomicBoolean isRunning, isController;
    private final ServerSocketChannel serverChannel;
    private final ExecutorService executor;
    private final ConcurrentHashMap<Integer, BrokerInfo> clusterMetadata;
    private final ZooKeeperClient zkClient;

    public SimpleKafkaBroker(int brokerId, String brokerHost, int brokerPort, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.zkClient = new ZooKeeperClient("localhost", zkPort);
        this.topics = new ConcurrentHashMap<>();
        this.serverChannel = ServerSocketChannel.open();
        this.clusterMetadata = new ConcurrentHashMap<>();
        this.isController = new AtomicBoolean(false);
        this.isRunning = new AtomicBoolean(false);
        this.executor = Executors.newFixedThreadPool(10);

        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if(!dataDir.exists()) dataDir.mkdirs();

    }
    public void start() throws IOException {
        if(isRunning.compareAndSet(false, true)) {
            serverChannel.socket().bind(new InetSocketAddress(brokerHost, brokerPort));
            serverChannel.configureBlocking(false);
            LOGGER.info("SimpleKafka broker started on " + brokerHost + ":" + brokerPort);

            registerWithZookeeper();
            electController();
            loadTopics();
            executor.submit(this::acceptConnections);
        }
    }


    private void electController() {
        String controllerPath = "/controller";
        try {
            boolean nodeExists = zkClient.exists(controllerPath);

            if(nodeExists) {
                String existingData = zkClient.getData(controllerPath);
                if(existingData == null || existingData.trim().isEmpty()) {
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                }
            }
            boolean becomeController = false;
            if(!nodeExists) {
                becomeController = zkClient.createEphemeralNodes(controllerPath, String.valueOf(brokerId));
            }
            if(becomeController) {
                isController.set(true);
                rebalancePartitions();
            } else {
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }


        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed", e);
            new Thread(() -> {
               try {
                   Thread.sleep(2000);
                   electController();
               } catch (InterruptedException ex) {
                   Thread.currentThread().interrupt();
               }
            }).start();
        }

    }

    private void onControllerChange() {
        LOGGER.info("Controller changed, initiating new election");
        electController();
    }

    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        if(!isController.get()) {
            LOGGER.warning("Only the controller can create topics");
            return;
        }

        try {
            String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
            new File(topicDir).mkdirs();

            String topicPath = "/topic/" + topic;
            if(!zkClient.exists(topicPath)) {
                zkClient.createPersistentNodes(topicPath, "");
                zkClient.createPersistentNodes(topicPath + "/partitions", "");
            }

            List<Partition> partitions = new ArrayList<>();
            List<Integer> brokerIds = new ArrayList<>(clusterMetadata.keySet());

            for(int i = 0; i < numPartitions; i++) {
                int partitionId = i;
                String partitionDir = topicDir + File.separator + partitionId;
                new File(partitionDir).mkdirs();
            }
        } catch(Exception e) {

        }


    }

    public void loadTopics() {
        try {
          List<String> children = zkClient.getChildren("/topics");

          for(String topic: children) {
              try {
                  loadTopic(topic);
              } catch(Exception e) {
                  LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e);
              }
          }
        } catch(Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topics", e);

        }
    }

    private void loadTopic(String topic) throws Exception {
        if(topics.containsKey(topic)) {
            LOGGER.info("Topic already loaded: " + topic);
            return;
        }
        String topicPath = "/topics/" + topic;

        if(!zkClient.exists(topic)) {
            throw new Exception("Topic does not exist in ZooKeeper: " + topic);
        }
        String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;

        new File(topicDir).mkdirs();
        List<String> partitionIds = zkClient.getChildren(topicPath + "/partitions");
        List<Partition> partitions = new ArrayList<>();
        for(String partitionId: partitionIds) {
            int id = Integer.parseInt(partitionId);
            String partitionPath = topicPath + "/partitions/" + partitionId;
            String partitionData = zkClient.getData(partitionPath);

            String[] parts = partitionData.split(";");
            int leader = Integer.parseInt(parts[0]);

            List<Integer> followers = new ArrayList<>();
            if(parts.length > 1 && !parts[0].isEmpty()) {
                String[] followerIds = parts[1].split(",");
                for(String followerId: followerIds) {
                    if(!followerId.isEmpty()) {
                        followers.add(Integer.parseInt(followerId));
                    }
                }
            }
            String partitionDir = topicDir + File.separator + id;
            new File(partitionDir).mkdirs();

            Partition partition = new Partition(id, leader, followers, partitionDir);
            partitions.add(partition);

            LOGGER.info("Loaded partition " + id + " for topic " + topic +
                    ", leader: " + leader + ", followers: " + followers);

        }
        topics.put(topic, partitions);
        LOGGER.info("Successfully loaded topic: " + topic + " with " + partitions.size() + " partitions");
    }

    public void stop() {
        if(isRunning.compareAndSet(true, false)) {
            try {
                LOGGER.info("Stopping SimpleKafka broker...");
                serverChannel.close();
                for(List<Partition> list: topics.values()) {
                    for(Partition p: list) {
                        p.close();
                    }
                }
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch(Exception e) {
                LOGGER.log(Level.SEVERE, "Error stopping broker", e);
            }
        }
    }

    private void registerWithZookeeper() {
        try {
            zkClient.connect();
            String brokerPath = "/brokers/" + brokerId;
            String brokerData = brokerHost + ":" + brokerPort;
            zkClient.createEphemeralNodes(brokerPath, brokerData);

            BrokerInfo selfInfo = new BrokerInfo(brokerId, brokerHost, brokerPort);
            clusterMetadata.put(brokerId, selfInfo);
            zkClient.watchChildren("/brokers", this::onBrokersChanged);
            LOGGER.info("Registered with ZooKeeper at " + zkClient.getConnectString());

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
        }

    }
    private void onBrokersChanged(List<String> brokerIds) {
        LOGGER.info("Broker change detected. Current brokers: " + brokerIds);

        for(String id: brokerIds) {
            try {
                int brokerId = Integer.parseInt(id);
                if(!clusterMetadata.containsKey(brokerId)) {
                    String brokerData = zkClient.getData("/broker/" + id);
                    String[] hostPort = brokerData.split(":");
                    BrokerInfo info = new BrokerInfo(
                            brokerId,
                            hostPort[0],
                            Integer.parseInt(hostPort[1])
                    );
                    clusterMetadata.put(brokerId, info);
                    LOGGER.info("Added broker: " + info);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        List<Integer> toRemove = new ArrayList<>();

        for(Integer brokerId: clusterMetadata.keySet()) {
            if(!brokerIds.contains(String.valueOf(brokerId))) {
                toRemove.add(brokerId);
            }
        }

        for(Integer brokerId: toRemove) {
            clusterMetadata.remove(brokerId);
            LOGGER.info("Removed broker: " + brokerId);
        }

        if(!brokerIds.contains(String.valueOf(brokerId)) && isController.get()) {
            isController.set(false);
            LOGGER.info("This broker is no longer in the cluster, giving up controller status");
        } else if(isController.get()) {
            rebalancePartitions();
        } else {
            electController();
        }
    }

    private void rebalancePartitions() {
        if(!isController.get()) {
            return;
        }
        LOGGER.info("Rebalancing partitions across cluster");
        for(Map.Entry<String, List<Partition>> entry: topics.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();
            for(Partition p: partitions) {
                if(p.getLeader() == -1 || !clusterMetadata.containsKey(p.getLeader())) {
                    List<Integer> broker = new ArrayList<>(clusterMetadata.keySet());
                    if(!broker.isEmpty()) {
                        int newLeader = broker.getFirst();
                        p.setLeader(newLeader);

                        List<Integer> followers = new ArrayList<>();
                        for(int i = 1; i < Math.min(broker.size(), 3); i++) {
                            followers.add(broker.get(i));
                        }
                        p.setFollowers(followers);
                        updatePartitionMetadata(topic, p);

                        LOGGER.info("Reassigned partition " + p.getId() +
                                " of topic " + topic +
                                " to leader " + newLeader +
                                " with followers " + followers);
                    }
                }
            }

        }
    }

    private void updatePartitionMetadata(String topic, Partition partition) {
        try {

            StringBuilder pathBuilder = new StringBuilder("/topics/");
            pathBuilder.append(topic)
                    .append("/partitions/")
                    .append(partition.getId());
            StringBuilder dataBuilder = new StringBuilder(partition.getLeader());
            dataBuilder.append(";");
            for (int follower : partition.getFollowers()) {
                dataBuilder.append(follower).append(",");
            }

            String path = pathBuilder.toString();
            String data = dataBuilder.toString();

            if (zkClient.exists(path)) {
                zkClient.setData(path, data);
            } else {
                zkClient.createPersistentNodes(path, data);
            }
        } catch(Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update partition metadata", e);
        }


    }

    private void acceptConnections() {
        
    }
}
