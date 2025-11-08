package pt.isec.pdG36.proj.server;

import java.io.*;
import java.net.*;

public class Server {

    private final InetAddress dirAddr;
    private final int dirUdpPort;
    private final File dbDirectory;
    private final InetAddress multicastIface;

    private ServerSocket clientServerSocket;
    private ServerSocket dbServerSocket;

    private InetAddress primaryDbAddr;
    private int primaryDbPort;
    private boolean isPrimary;

    public Server(InetAddress dirAddr, int dirUdpPort, File dbDirectory, InetAddress multicastIface) {
        this.dirAddr = dirAddr;
        this.dirUdpPort = dirUdpPort;
        this.dbDirectory = dbDirectory;
        this.multicastIface = multicastIface;
    }

    public void start() throws Exception {
        // Create TCP listening sockets on automatic ports
        clientServerSocket = new ServerSocket(0);
        dbServerSocket = new ServerSocket(0);

        int clientPort = clientServerSocket.getLocalPort();
        int dbPort = dbServerSocket.getLocalPort();

        System.out.println("Server TCP ports - clients: " + clientPort + ", dbCopy: " + dbPort);

        // Register via UDP
        if (!registerWithDirectory(clientPort, dbPort)) {
            System.err.println("Failed to register with directory. Exiting.");
            return;
        }

        // DB initialization / sync (skeleton)
        if (isPrimary) {
            initOrLoadLocalDbAsPrimary();
        } else {
            obtainDbFromPrimary();
        }

        // Start threads
        startClientAcceptor();
        startDbCopyAcceptor();
        startHeartbeatSender(clientPort, dbPort);
        startHeartbeatMulticastListener(); // to be used for sync checks later

        // For now: block main thread forever
        // In real code, handle shutdown etc.
    }

    private boolean registerWithDirectory(int clientPort, int dbPort) {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(3000);
            String msg = "REGISTER " + clientPort + " " + dbPort;
            byte[] data = msg.getBytes();

            DatagramPacket packet = new DatagramPacket(data, data.length, dirAddr, dirUdpPort);
            socket.send(packet);

            byte[] buf = new byte[256];
            DatagramPacket resp = new DatagramPacket(buf, buf.length);
            socket.receive(resp);

            String reply = new String(resp.getData(), 0, resp.getLength()).trim();
            // Expected: PRIMARY <ip> <dbPort>
            String[] parts = reply.split("\\s+");
            if (parts.length == 3 && "PRIMARY".equals(parts[0])) {
                primaryDbAddr = InetAddress.getByName(parts[1]);
                primaryDbPort = Integer.parseInt(parts[2]);

                // If primary info matches us -> we are primary
                isPrimary = primaryDbAddr.equals(InetAddress.getLocalHost())
                        && primaryDbPort == dbPort;
                System.out.println("Primary is " + primaryDbAddr.getHostAddress() + ":" + primaryDbPort
                        + " | isPrimary=" + isPrimary);
                return true;
            } else {
                System.err.println("Unexpected directory reply: " + reply);
                return false;
            }
        } catch (IOException e) {
            System.err.println("Error registering with directory: " + e.getMessage());
            return false;
        }
    }

    private void initOrLoadLocalDbAsPrimary() {
        // TODO:
        // - scan dbDirectory for .db files
        // - choose/create latest as version 0+...
        System.out.println("Initializing/choosing local DB as primary (TODO).");
    }

    private void obtainDbFromPrimary() {
        // TODO:
        // - connect via TCP to primaryDbAddr:primaryDbPort
        // - receive .db file and store in dbDirectory
        System.out.println("Obtaining DB from primary " + primaryDbAddr + ":" + primaryDbPort + " (TODO).");
    }

    private void startClientAcceptor() {
        Thread t = new Thread(() -> {
            System.out.println("Client acceptor running...");
            while (true) {
                try {
                    Socket client = clientServerSocket.accept();
                    // TODO: spawn handler thread (auth, commands, etc.)
                    System.out.println("Client connected from " + client.getRemoteSocketAddress());
                    client.close(); // placeholder
                } catch (IOException e) {
                    System.err.println("Client accept error: " + e.getMessage());
                    break;
                }
            }
        }, "ClientAcceptor");
        t.start();
    }

    private void startDbCopyAcceptor() {
        Thread t = new Thread(() -> {
            System.out.println("DB copy acceptor running...");
            while (true) {
                try {
                    Socket s = dbServerSocket.accept();
                    // TODO: send or receive DB file as needed
                    s.close(); // placeholder
                } catch (IOException e) {
                    System.err.println("DB accept error: " + e.getMessage());
                    break;
                }
            }
        }, "DbCopyAcceptor");
        t.start();
    }

    private void startHeartbeatSender(int clientPort, int dbPort) {
        Thread t = new Thread(() -> {
            try (DatagramSocket udp = new DatagramSocket()) {
                InetAddress mcastAddr = InetAddress.getByName("230.30.30.30");
                int mcastPort = 3030;
                while (true) {
                    // minimal heartbeat (no SQL yet)
                    String msg = "HEARTBEAT " + clientPort + " " + dbPort + " " + /*dbVersion*/ 0;
                    byte[] data = msg.getBytes();

                    // to directory
                    udp.send(new DatagramPacket(data, data.length, dirAddr, dirUdpPort));
                    // to multicast group
                    udp.send(new DatagramPacket(data, data.length, mcastAddr, mcastPort));

                    Thread.sleep(5000);
                }
            } catch (Exception e) {
                System.err.println("Heartbeat sender stopped: " + e.getMessage());
            }
        }, "HeartbeatSender");
        t.setDaemon(true);
        t.start();
    }

    private void startHeartbeatMulticastListener() {
        // Skeleton: join group and read; later you implement logic from spec.
        Thread t = new Thread(() -> {
            try (MulticastSocket mcast = new MulticastSocket(3030)) {
                InetAddress group = InetAddress.getByName("230.30.30.30");
                mcast.joinGroup(new InetSocketAddress(group, 3030),
                        NetworkInterface.getByInetAddress(multicastIface));

                byte[] buf = new byte[512];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                while (true) {
                    mcast.receive(packet);
                    String msg = new String(packet.getData(), 0, packet.getLength()).trim();
                    // TODO: ignore our own & non-primary; process version/SQL as per spec
                }
            } catch (IOException e) {
                System.err.println("Multicast listener stopped: " + e.getMessage());
            }
        }, "HeartbeatMulticastListener");
        t.setDaemon(true);
        t.start();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Usage: java PDServer <dirIP> <dirUdpPort> <dbDir> <multicastInterfaceIP>");
            return;
        }
        InetAddress dirAddr = InetAddress.getByName(args[0]);
        int dirPort = Integer.parseInt(args[1]);
        File dbDir = new File(args[2]);
        InetAddress mcIf = InetAddress.getByName(args[3]);

        if (!dbDir.exists() && !dbDir.mkdirs()) {
            System.err.println("Could not create dbDir: " + dbDir.getAbsolutePath());
            return;
        }

        new Server(dirAddr, dirPort, dbDir, mcIf).start();
    }
}

