package pt.isec.pdG36.proj.directory;

import pt.isec.pdG36.proj.directory.core.ServerRegistry;
import pt.isec.pdG36.proj.directory.model.ServerInfo;
import pt.isec.pdG36.proj.common.protocol.DirectoryProtocol;

import java.io.IOException;
import java.net.*;

public class DirectoryService {
    private static final long HEARTBEAT_TIMEOUT_MS = 17_000;

    private final int udpPort;
    private DatagramSocket socket;
    private final ServerRegistry registry;

    public DirectoryService(int udpPort, ServerRegistry registry) {
        this.udpPort = udpPort;
        this.registry = registry;
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
        if (msg.isEmpty()) return;

        String[] parts = msg.split("\\s+");
        String cmd = parts[0];

        InetAddress srcAddr = packet.getAddress();
        int srcPort = packet.getPort();

        switch (cmd) {
            case "REGISTER" -> onRegister(parts, srcAddr, srcPort);
            case "HEARTBEAT" -> onHeartbeat(msg, srcAddr, srcPort);
            case "UNREGISTER" -> onUnregister(parts, srcAddr);
            case "GET_SERVER" -> onGetServer(srcAddr, srcPort);
            default -> {
                // ignore invalid
            }
        }
    }

    private void onRegister(String[] parts, InetAddress addr, int replyPort) throws IOException {
        if (parts.length < 3) return;
        int clientPort = Integer.parseInt(parts[1]);
        int dbPort = Integer.parseInt(parts[2]);

        ServerInfo info = registry.register(addr, clientPort, dbPort);
        System.out.printf("Registered server %s:%d (db:%d)%n",
                info.getAddress().getHostAddress(),
                info.getClientTcpPort(),
                info.getDbTcpPort());

        ServerInfo primary = registry.getPrimary();
        if (primary != null) {
            String resp = DirectoryProtocol.buildPrimaryReply(
                    primary.getAddress().getHostAddress(),
                    primary.getDbTcpPort()
            );
            sendUdp(resp, addr, replyPort);
        }
    }

    private void onHeartbeat(String msg, InetAddress addr, int replyPort) {
        DirectoryProtocol.HeartbeatInfo hb = DirectoryProtocol.parseHeartbeat(msg);
        if (hb == null) {
            System.err.println("[DIR] Invalid HEARTBEAT from " + addr + " -> " + msg);
            return;
        }

        registry.heartbeat(
                addr,
                hb.clientPort(),
                hb.dbPort(),
                System.currentTimeMillis()
        );

        // update primary if needed
        ServerInfo primary = registry.getPrimary();
        if (primary != null) {
            String resp = DirectoryProtocol.buildPrimaryReply(
                    primary.getAddress().getHostAddress(),
                    primary.getDbTcpPort()
            );
            try {
                sendUdp(resp, addr, replyPort);
            } catch (IOException e) {
                System.err.println("[DIR] Failed to reply to HEARTBEAT: " + e.getMessage());
            }
        }
    }

    private void onUnregister(String[] parts, InetAddress addr) {
        if (parts.length < 3) return;
        int clientPort = Integer.parseInt(parts[1]);
        int dbPort = Integer.parseInt(parts[2]);
        registry.unregister(addr, clientPort, dbPort);
    }

    private void onGetServer(InetAddress addr, int replyPort) throws IOException {
        ServerInfo primary = registry.getPrimary();
        String resp = (primary == null)
                ? DirectoryProtocol.buildNoServerReply()
                : DirectoryProtocol.buildServerReply(
                primary.getAddress().getHostAddress(),
                primary.getClientTcpPort()
        );
        sendUdp(resp, addr, replyPort);
    }

    private void cleanupLoop() {
        while (true) {
            try {
                Thread.sleep(1000);
                long now = System.currentTimeMillis();
                registry.cleanupExpired(now, HEARTBEAT_TIMEOUT_MS);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void sendUdp(String msg, InetAddress addr, int port) throws IOException {
        byte[] data = msg.getBytes();
        DatagramPacket p = new DatagramPacket(data, data.length, addr, port);
        socket.send(p);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: <udpPort>");
            return;
        }
        int udpPort = Integer.parseInt(args[0]);
        ServerRegistry registry = new ServerRegistry();
        new DirectoryService(udpPort, registry).start();
    }
}
