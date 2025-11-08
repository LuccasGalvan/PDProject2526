package pt.isec.pdG36.proj.client;

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

    public boolean connectToPrimary() {
        ServerEndpoint ep = requestPrimaryFromDirectory();
        if (ep == null) {
            System.err.println("No server available.");
            return false;
        }

        try {
            serverSocket = new Socket(ep.host, ep.port);
            serverSocket.setSoTimeout(30_000); // for auth phase
            in = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
            out = new PrintWriter(serverSocket.getOutputStream(), true);
            System.out.println("Connected to server " + ep.host + ":" + ep.port);
            return true;
        } catch (IOException e) {
            System.err.println("Failed to connect to server: " + e.getMessage());
            return false;
        }
    }

    private ServerEndpoint requestPrimaryFromDirectory() {
        try (DatagramSocket udp = new DatagramSocket()) {
            udp.setSoTimeout(2000);
            byte[] data = "GET_SERVER".getBytes();
            DatagramPacket req = new DatagramPacket(data, data.length, dirAddr, dirUdpPort);
            udp.send(req);

            byte[] buf = new byte[256];
            DatagramPacket resp = new DatagramPacket(buf, buf.length);
            udp.receive(resp);

            String msg = new String(resp.getData(), 0, resp.getLength()).trim();
            String[] parts = msg.split("\\s+");
            if (parts.length == 3 && "SERVER".equals(parts[0])) {
                String host = parts[1];
                int port = Integer.parseInt(parts[2]);
                return new ServerEndpoint(host, port);
            }
            return null;
        } catch (IOException e) {
            System.err.println("Error contacting directory: " + e.getMessage());
            return null;
        }
    }

    public void sendLine(String line) {
        if (out != null) out.println(line);
    }

    public String readLine() throws IOException {
        if (in == null) return null;
        return in.readLine();
    }

    public void close() {
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
    }

    private record ServerEndpoint(String host, int port) {}
}
