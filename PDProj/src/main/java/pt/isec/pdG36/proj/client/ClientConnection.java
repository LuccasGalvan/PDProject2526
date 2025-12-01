package pt.isec.pdG36.proj.client;

import pt.isec.pdG36.proj.common.protocol.DirectoryProtocol;

import java.io.*;
import java.net.*;

public class ClientConnection {

    private final InetAddress dirAddr;
    private final int dirUdpPort;

    private Socket serverSocket;
    private BufferedReader in;
    private PrintWriter out;

    public ClientConnection(InetAddress dirAddr, int dirUdpPort) {
        this.dirAddr = dirAddr;
        this.dirUdpPort = dirUdpPort;
    }

    //public API

    public boolean connectToPrimary() {
        for (int attempt = 1; attempt <= 10; attempt++) {
            DirectoryProtocol.ServerInfoResp ep = requestPrimaryFromDirectory();
            if (ep == null) {
                System.err.println("[CLIENT] No server available (attempt " + attempt + ")");
                sleep(1000);
                continue;
            }

            if (connectToServer(ep.ip(), ep.clientPort())) {
                return true;
            }

            System.err.println("[CLIENT] Failed to connect to suggested server (attempt " + attempt + ")");
            sleep(1000);
        }

        return false;
    }

    public boolean reconnectToPrimary() {
        closeSilently();
        return connectToPrimary();
    }

    public void sendLine(String line) {
        if (out != null) {
            out.println(line);
        }
    }

    public String readLine() throws IOException {
        if (in == null) return null;
        return in.readLine();
    }

    public void close() {
        closeSilently();
    }

    //helpers

    private DirectoryProtocol.ServerInfoResp requestPrimaryFromDirectory() {
        try (DatagramSocket udp = new DatagramSocket()) {
            udp.setSoTimeout(2000);

            String msg = DirectoryProtocol.buildGetServer();
            byte[] data = msg.getBytes();

            DatagramPacket req = new DatagramPacket(data, data.length, dirAddr, dirUdpPort);
            udp.send(req);

            byte[] buf = new byte[256];
            DatagramPacket resp = new DatagramPacket(buf, buf.length);
            udp.receive(resp);

            String reply = new String(resp.getData(), 0, resp.getLength()).trim();

            if (DirectoryProtocol.isNoServer(reply)) {
                return null;
            }

            DirectoryProtocol.ServerInfoResp info = DirectoryProtocol.parseServerReply(reply);
            if (info == null) {
                System.err.println("[CLIENT] Invalid reply from directory: " + reply);
            }
            return info;

        } catch (SocketTimeoutException e) {
            System.err.println("[CLIENT] Directory timeout.");
            return null;
        } catch (IOException e) {
            System.err.println("[CLIENT] Error talking to directory: " + e.getMessage());
            return null;
        }
    }

    private boolean connectToServer(String host, int port) {
        try {
            System.out.println("[CLIENT] Connecting to server " + host + ":" + port + " ...");
            this.serverSocket = new Socket(host, port);
            this.serverSocket.setSoTimeout(30_000);

            this.in = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
            this.out = new PrintWriter(serverSocket.getOutputStream(), true);

            System.out.println("[CLIENT] Connected.");
            return true;
        } catch (IOException e) {
            System.err.println("[CLIENT] Failed to connect to server: " + e.getMessage());
            closeSilently();
            return false;
        }
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }

    private void closeSilently() {
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
        serverSocket = null;
        in = null;
        out = null;
    }

    public String safeReadLine() {
        try {
            return readLine();
        } catch (IOException e) {
            System.err.println("[CLIENT] Lost connection to current server: " + e.getMessage());
            System.err.println("[CLIENT] Attempting to reconnect to new primary...");

            if (reconnectToPrimary()) {
                System.out.println("[CLIENT] Reconnected to new primary. Please retry the command.");
            } else {
                System.err.println("[CLIENT] Could not reconnect. Cluster may be unstable.");
            }

            return null; // caller detects null and returns to menu gracefully
        }
    }

    private record ServerEndpoint(String host, int port) {}
}
