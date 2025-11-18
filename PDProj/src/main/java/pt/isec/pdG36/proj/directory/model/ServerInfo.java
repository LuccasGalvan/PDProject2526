package pt.isec.pdG36.proj.directory.model;

import java.net.InetAddress;

// Server identity: IP + ports
public class ServerInfo {
    final InetAddress address;
    final int clientTcpPort;
    final int dbTcpPort;
    volatile long lastHeartbeat; // timestamp in ms

    public ServerInfo(InetAddress address, int clientTcpPort, int dbTcpPort, long l) {
        this.address = address;
        this.clientTcpPort = clientTcpPort;
        this.dbTcpPort = dbTcpPort;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getClientTcpPort() {
        return clientTcpPort;
    }

    public int getDbTcpPort() {
        return dbTcpPort;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(long lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public String key() {
        return key(address, clientTcpPort, dbTcpPort);
    }

    public static String key(InetAddress addr, int clientTcpPort, int dbTcpPort) {
        return addr.getHostAddress() + ":" + clientTcpPort + ":" + dbTcpPort;
    }
}