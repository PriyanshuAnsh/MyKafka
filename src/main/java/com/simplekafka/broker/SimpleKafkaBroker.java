package com.simplekafka.broker;

import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
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

            registerWithZooKeeper();
            electController();
            loadTopics();
            executor.submit(this::acceptConnections);
        }
    }

    private void registerWithZooKeeper() {
    }

    private void electController() {
        
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

    private void acceptConnections() {
        
    }
}
