package pt.isec.pdG36.proj.directory.core;

import pt.isec.pdG36.proj.directory.model.ServerInfo;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServerRegistry {
    private static final long HEARTBEAT_TIMEOUT_MS = 17_000;

    // For quick lookup
    public final Map<String, ServerInfo> serversByKey = new ConcurrentHashMap<>();

    // For FIFO order of registration
    public final LinkedList<ServerInfo> orderedServers = new LinkedList<>();

    public synchronized ServerInfo register(InetAddress addr, int clientTcpPort, int dbTcpPort) {
        String key = ServerInfo.key(addr, clientTcpPort, dbTcpPort);
        ServerInfo existing = serversByKey.get(key);
        if (existing != null) {
            return existing;
        }

        ServerInfo info = new ServerInfo(addr, clientTcpPort, dbTcpPort, System.currentTimeMillis());
        serversByKey.put(key, info);
        orderedServers.addLast(info);
        return info;
    }

    public synchronized void heartbeat(InetAddress addr, int clientTcpPort, int dbTcpPort, long now) {
        String key = ServerInfo.key(addr, clientTcpPort, dbTcpPort);
        ServerInfo info = serversByKey.get(key);
        if (info != null) {
            info.setLastHeartbeat(now);
        }
        // if null: ignore (not auto-registering)
    }

    public synchronized void unregister(InetAddress addr, int clientTcpPort, int dbTcpPort) {
        String key = ServerInfo.key(addr, clientTcpPort, dbTcpPort);
        ServerInfo info = serversByKey.remove(key);
        if (info != null) {
            orderedServers.remove(info);
        }
    }

    public synchronized void cleanupExpired(long now, long timeoutMs) {
        Iterator<ServerInfo> it = orderedServers.iterator();
        while (it.hasNext()) {
            ServerInfo s = it.next();
            if (now - s.getLastHeartbeat() > timeoutMs) {
                serversByKey.remove(s.key());
                it.remove();
            }
        }
    }

    public synchronized ServerInfo getPrimary() {
        return orderedServers.peekFirst();
    }
}
