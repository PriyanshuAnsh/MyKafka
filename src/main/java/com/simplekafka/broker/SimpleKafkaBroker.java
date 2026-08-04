package com.simplekafka.broker;

import io.netty.buffer.ByteBuf;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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
                int leaderIdx = i % brokerIds.size();
                int leaderId = brokerIds.get(leaderIdx);

                List<Integer> followers = new ArrayList<>();
                for(int j = 1; j < replicationFactor; i++) {
                    int followerIdx = (j + leaderIdx) % brokerIds.size();
                    followers.add(brokerIds.get(followerIdx));
                }

                Partition partition = new Partition(partitionId, leaderId, followers, partitionDir);
                partitions.add(partition);

                String partitionPath = topicPath + "/partitions/" + partitionId;
                StringBuilder partitionDataBuilder = new StringBuilder(leaderId + ";");

                for(int f: followers) {
                    partitionDataBuilder.append(f).append(",");
                }
                String partitionData = partitionDataBuilder.toString();

                zkClient.createPersistentNodes(partitionPath, partitionData);
                LOGGER.info("Created partition " + partitionId +
                        " for topic " + topic +
                        " with leader " + leaderId +
                        " and followers " + followers);
            }

            topics.put(topic, partitions);

            for(int i: brokerIds) {
                if(i != this.brokerId) notifyBrokerForTopicCreation(brokerId, topic);
            }
        } catch(Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create topic", e);
        }
    }

    private void notifyBrokerForTopicCreation(int brokerId, String topic) {
        BrokerInfo broker = clusterMetadata.get(brokerId);
        if(broker == null) return;
        executor.submit(() -> {
           try(SocketChannel brokerChannel = SocketChannel.open()) {
               brokerChannel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));
               ByteBuffer request = ByteBuffer.allocate(3 + topic.length());
               request.put(Protocol.TOPIC_NOTIFICATION);
               request.putShort((short) topic.length());
               request.put(topic.getBytes());
               request.flip();
               brokerChannel.write(request);
               ByteBuffer response = ByteBuffer.allocate(1);
               brokerChannel.read(response);
           } catch(Exception e) {
               LOGGER.log(Level.WARNING, "Failed to notify broker " + brokerId + " about topic creation", e);
           }
        });
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

    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicByte = new byte[topicLength];
        buffer.get(topicByte);
        String topic = new String(topicByte);

        int partition = buffer.getInt();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        LOGGER.info("Produce request for topic: " + topic + ", partition: " + partition);

        if(!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }
        List<Partition> partitions = topics.get(topic);
        Partition target = null;

        for(Partition p: partitions) {
            if(p.getId() == partition) {
                target = p;
                break;
            }
        }

        if (target == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        if(target.getLeader() != brokerId) {
            forwardProduceToLeader(clientChannel, topic, partition, message, target.getLeader());
            return;
        }

        long offset = target.append(message);
        replicateToFollowers(topic, target, message, offset);

        ByteBuffer response = ByteBuffer.allocate(10);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.put((byte) (offset > -1 ? 0 : 1)); // 0 = success, 1 = error
        response.flip();
        clientChannel.write(response);


    }

    private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
        for(int followerId: partition.getFollowers()) {
            if(followerId == brokerId) continue;
            BrokerInfo follower = clusterMetadata.get(followerId);
            if(follower == null) continue;
            executor.submit(() -> {
                try(SocketChannel followerChannel = SocketChannel.open()) {
                    followerChannel.connect(new InetSocketAddress(follower.getHost(), follower.getPort()));
                    ByteBuffer request = ByteBuffer.allocate(17 + topic.length() + message.length);
                    request.put(Protocol.REPLICATE);
                    request.putShort((short) topic.length());
                    request.put(topic.getBytes());
                    request.putInt(partition.getId());
                    request.putLong(offset);
                    request.putInt(message.length);
                    request.put(message);
                    request.flip();

                    followerChannel.write(request);
                    ByteBuffer response = ByteBuffer.allocate(1);
                    followerChannel.read(response);

                    response.flip();

                    byte ack = response.get();
                    LOGGER.info("Replication to follower " + followerId + " " +
                            (ack == Protocol.REPLICATE_ACK ? "succeeded" : "failed"));
                } catch(IOException e) {
                    LOGGER.log(Level.SEVERE, "Replication to follower " + followerId + " failed", e);
                }
            });

        }
    }

    private void forwardProduceToLeader(SocketChannel clientChannel, String topic, int partition, byte[] message, int leader) {

    }

    private void acceptConnections() {
        
    }
}
