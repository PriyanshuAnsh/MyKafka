package com.simplekafka.broker;

import java.nio.ByteBuffer;

/**
 * This class is the foundation of `MyKafka` wire protocol implementation.
 * It defines how Broker and Client communicate over the network.
 *
 * @author Priyanshu Dongre
 */
public class Protocol {

    //Client Request Types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte META = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    //Broker Response Types
    public static final int PRODUCE_RESPONSE = 0x11;
    public static final int FETCH_RESPONSE = 0x12;


    /**
     * Encodes the message to produce/write to the topic.
     * @param topic Topic Name
     * @param partition Partition ID
     * @param message Message Content
     * @return ByteBuffer instance
     */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        return null;
    }

    /**
     * Encodes a request to fetch/read messages from a specific topic,
     * partition, starting from a given offset.
     * @param topic Topic Name
     * @param partition Partition ID
     * @param offset Dtarting offset
     * @param maxBytes Max Bytes to Fetch
     * @return ByteBuffer
     */
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        return null;
    }

    /**
     * Encode a request to retrieve metadata about broker and the topics in the cluster.
     * @return ByteBuffer
     */
    public static ByteBuffer encodeMetadataRequest() {
        return null;
    }

    /**
     * Encodes the request to create a new topic with specific partitions and replications.
     * @param topic Topic Name
     * @param numPartitions Number of partitions
     * @param replicationFactor Replication factor
     * @return ByteBuffer
     */
    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        return null;
    }

}
