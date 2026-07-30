package com.simplekafka.broker;

import java.util.Objects;

/**
 * Hold information about the Broker in MyKafka Cluster
 * @author Priyanshu Dongre
 */
public class BrokerInfo {
    private final int id;
    private final String host;
    private final int port;

    public BrokerInfo(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return "BrokerInfo{" +
                "id=" + id +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        BrokerInfo that = (BrokerInfo) o;
        return getId() == that.getId() && getPort() == that.getPort() && Objects.equals(getHost(), that.getHost());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getHost(), getPort());
    }
}
