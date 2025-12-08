package pt.isec.pdG36.proj.client;

import pt.isec.pdG36.proj.common.protocol.DirectoryProtocol;

import java.io.*;
import java.net.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ClientConnection {

    private final InetAddress dirAddr;
    private final int dirUdpPort;

    private Socket serverSocket;
    private BufferedReader in;
    private PrintWriter out;

    private Thread receiverThread;
    private final BlockingQueue<String> responseQueue = new LinkedBlockingQueue<>();
    private volatile boolean receiverRunning = false;

    public ClientConnection(InetAddress dirAddr, int dirUdpPort) {
        this.dirAddr = dirAddr;
        this.dirUdpPort = dirUdpPort;
    }

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

    public void sendLine(String line) {
        if (out != null) {
            out.println(line);
        }
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

            this.in = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
            this.out = new PrintWriter(serverSocket.getOutputStream(), true);

            startReceiverThread();

            System.out.println("[CLIENT] Connected.");
            return true;
        } catch (IOException e) {
            System.err.println("[CLIENT] Failed to connect to server: " + e.getMessage());
            closeSilently();
            return false;
        }
    }

    private void startReceiverThread() {
        receiverRunning = true;
        receiverThread = new Thread(() -> {
            try {
                String line;
                while (receiverRunning && (line = in.readLine()) != null) {
                    // Trim once here
                    line = line.trim();
                    if (line.startsWith("NOTIFY ")) {
                        String payload = line.substring("NOTIFY ".length());
                        // Simple async notification print
                        System.out.println();
                        System.out.println("[NOTIFICATION] " + payload);
                    } else {
                        // enqueue normal responses for whoever is waiting
                        responseQueue.put(line);
                    }
                }
            } catch (IOException e) {
                if (receiverRunning) {
                    if (!(e instanceof java.net.SocketException &&
                            //ignore common "socket closed" on shutdown
                            "Socket closed".equalsIgnoreCase(e.getMessage()))) {
                        System.err.println("[CLIENT] Receiver thread IO error: " + e.getMessage());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                receiverRunning = false;
            }
        }, "ServerReceiver");
        receiverThread.setDaemon(true);
        receiverThread.start();
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }

    private void closeSilently() {
        receiverRunning = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
        serverSocket = null;
        in = null;
        out = null;
        responseQueue.clear();
    }

    public String safeReadLine() {
        try {
            while (true) {
                // If the receiver died and there is nothing queued, treat as closed
                if (!receiverRunning && responseQueue.isEmpty()) {
                    return null;
                }

                String line = responseQueue.poll(1000, TimeUnit.MILLISECONDS);
                if (line != null) {
                    return line;
                }
                // else: timeout, loop again and re-check receiverRunning
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
