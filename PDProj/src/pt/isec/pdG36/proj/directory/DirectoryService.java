package pt.isec.pdG36.proj.directory;

import java.io.IOException;
import java.net.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DirectoryService {

    private static final long HEARTBEAT_TIMEOUT_MS = 17_000;

    private final int udpPort;
    private DatagramSocket socket;

    // Server identity: IP + ports
    private static class ServerInfo {
        final InetAddress address;
        final int clientTcpPort;
        final int dbTcpPort;
        volatile long lastHeartbeat; // timestamp in ms

        ServerInfo(InetAddress address, int clientTcpPort, int dbTcpPort) {
            this.address = address;
            this.clientTcpPort = clientTcpPort;
            this.dbTcpPort = dbTcpPort;
            this.lastHeartbeat = System.currentTimeMillis();
        }
    }

    // For quick lookup
    private final Map<String, ServerInfo> serversByKey = new ConcurrentHashMap<>();

    // For FIFO order of registration
    private final LinkedList<ServerInfo> orderedServers = new LinkedList<>();

    public DirectoryService(int udpPort) {
        this.udpPort = udpPort;
    }

    public void start() throws IOException {
        socket = new DatagramSocket(udpPort);
        System.out.println("DirectoryService listening on UDP " + udpPort);

        // Cleanup thread
        Thread cleaner = new Thread(this::cleanupLoop, "Dir-Cleanup");
        cleaner.setDaemon(true);
        cleaner.start();

        // Main loop
        byte[] buf = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);

        while (true) {
            socket.receive(packet);
            handlePacket(packet);
        }
    }

    private void handlePacket(DatagramPacket packet) throws IOException {
        String msg = new String(packet.getData(), 0, packet.getLength()).trim();
        String[] parts = msg.split("\\s+");
        if (parts.length == 0) return;

        InetAddress srcAddr = packet.getAddress();
        int srcPort = packet.getPort();

        switch (parts[0]) {
            case "REGISTER" -> handleRegister(parts, srcAddr, srcPort);
            case "HEARTBEAT" -> handleHeartbeat(parts, srcAddr);
            case "UNREGISTER" -> handleUnregister(parts, srcAddr);
            case "GET_SERVER" -> handleGetServer(srcAddr, srcPort);
            default -> {
                // ignore unknown
            }
        }
    }

    private void handleRegister(String[] parts, InetAddress addr, int replyPort) throws IOException {
        if (parts.length < 3) return;
        int clientPort = Integer.parseInt(parts[1]);
        int dbPort = Integer.parseInt(parts[2]);

        String key = key(addr, clientPort, dbPort);

        synchronized (orderedServers) {
            if (!serversByKey.containsKey(key)) {
                ServerInfo info = new ServerInfo(addr, clientPort, dbPort);
                serversByKey.put(key, info);
                orderedServers.addLast(info);
                System.out.printf("Registered server %s:%d (db:%d) at %s%n",
                        addr.getHostAddress(), clientPort, dbPort, Instant.now());
            }
        }

        // Determine primary (oldest)
        ServerInfo primary = getPrimary();
        // Reply: PRIMARY <ip> <dbPort>
        if (primary != null) {
            String resp = "PRIMARY " + primary.address.getHostAddress() + " " + primary.dbTcpPort;
            byte[] data = resp.getBytes();
            DatagramPacket reply = new DatagramPacket(data, data.length, addr, replyPort);
            socket.send(reply);
        }
    }

    private void handleHeartbeat(String[] parts, InetAddress addr) {
        if (parts.length < 4) return;
        int clientPort = Integer.parseInt(parts[1]);
        int dbPort = Integer.parseInt(parts[2]);
        // parts[3] could be dbVersion; ignore for skeleton

        String key = key(addr, clientPort, dbPort);
        ServerInfo info = serversByKey.get(key);
        if (info != null) {
            info.lastHeartbeat = System.currentTimeMillis();
        }
        // If not registered: ignore
    }

    private void handleUnregister(String[] parts, InetAddress addr) {
        if (parts.length < 3) return;
        int clientPort = Integer.parseInt(parts[1]);
        int dbPort = Integer.parseInt(parts[2]);
        removeServer(key(addr, clientPort, dbPort));
    }

    private void handleGetServer(InetAddress addr, int replyPort) throws IOException {
        ServerInfo primary = getPrimary();
        String resp;
        if (primary == null) {
            resp = "NO_SERVER";
        } else {
            resp = "SERVER " + primary.address.getHostAddress() + " " + primary.clientTcpPort;
        }
        byte[] data = resp.getBytes();
        DatagramPacket reply = new DatagramPacket(data, data.length, addr, replyPort);
        socket.send(reply);
    }

    private ServerInfo getPrimary() {
        synchronized (orderedServers) {
            return orderedServers.peekFirst();
        }
    }

    private void cleanupLoop() {
        while (true) {
            try {
                Thread.sleep(1000);
                long now = System.currentTimeMillis();
                synchronized (orderedServers) {
                    Iterator<ServerInfo> it = orderedServers.iterator();
                    while (it.hasNext()) {
                        ServerInfo s = it.next();
                        if (now - s.lastHeartbeat > HEARTBEAT_TIMEOUT_MS) {
                            String key = key(s.address, s.clientTcpPort, s.dbTcpPort);
                            serversByKey.remove(key);
                            it.remove();
                            System.out.printf("Removed server %s:%d (timeout)%n",
                                    s.address.getHostAddress(), s.clientTcpPort);
                        }
                    }
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void removeServer(String key) {
        synchronized (orderedServers) {
            ServerInfo info = serversByKey.remove(key);
            if (info != null) {
                orderedServers.remove(info);
                System.out.printf("Unregistered server %s:%d%n",
                        info.address.getHostAddress(), info.clientTcpPort);
            }
        }
    }

    private String key(InetAddress addr, int clientPort, int dbPort) {
        return addr.getHostAddress() + ":" + clientPort + ":" + dbPort;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: java DirectoryService <udpPort>");
            return;
        }
        int udpPort = Integer.parseInt(args[0]);
        new DirectoryService(udpPort).start();
    }
}
