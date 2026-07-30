package com.simplekafka.broker;

/**
 * This class is the foundation of `MyKafka` wire protocol implementation.
 * It defines how Broker and Client communicate over the network.
 *
 * @author Priyanshu Dongre
 */
public class Protocol {

    // Client Request Types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte META = 0x03;
    public static final byte CREATE_TOPIC = 0x04;
}
