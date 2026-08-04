package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

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
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;

    // Internal Broker Communications
    public static final byte REPLICATE = 0x21;
    public static final byte REPLICATE_ACK = 0x22;
    public static final byte TOPIC_NOTIFICATION = 0x23;

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

    /**
     * Decodes a response from the Produce Request
     * @param byteBuffer ByteBuffer Response from ProduceRequest
     * @return ProduceResult object
     */
    public static ProduceResult decodeProduceResponse(ByteBuffer byteBuffer) {
        return null;
    }

    /**
     * Decodes a response from the Fetch Request
     * @param byteBuffer ByteBuffer Response from FetchRequest
     * @return FetchResult object
     */
    public static FetchResult decodeFetchResponse(ByteBuffer byteBuffer) {

        return null;
    }

    /**
     * Decodes a response from the Metadata Request
     * @param byteBuffer ByteBuffer Response from MetadataRequest
     * @return MetadataResult object
     */
    public static MetadataResult decodeMetadataResponse(ByteBuffer byteBuffer) {
        return null;
    }

    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        return null;
    }
    public static ByteBuffer encodeTopicNotification(String topic) {
        return null;
    }

    public static void sendErrorResponse(SocketChannel channel, String errorMessage) {

    }


    private static class ProduceResult {
        byte offset;
        String errorMessage;

    }

    private static class FetchResult {
        byte[] messages;
        String errorMessage;
    }

    private static class MetadataResult {
        BrokerInfo brokerInfo;
        TopicMetadata topicMetadata;
        String errorMessage;
    }

    private static class TopicMetadata {
        String topicName;
        PartitionMetadata partitionMetadata;
    }

    private static class PartitionMetadata {
        int partitionId;
        int leaderBrokerId;
        int replicaBrokerId;
    }
}
