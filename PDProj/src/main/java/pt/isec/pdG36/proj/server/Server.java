// language: java
package pt.isec.pdG36.proj.server;


import pt.isec.pdG36.proj.common.protocol.DirectoryProtocol;
import pt.isec.pdG36.proj.common.protocol.ClientServerProtocol;
import pt.isec.pdG36.proj.server.db.DatabaseManager;
import pt.isec.pdG36.proj.server.db.User;

import javax.xml.crypto.Data;
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

    private volatile long dbVersion = 0;

    private DatabaseManager dbManager;

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

            String regMsg = DirectoryProtocol.buildRegister(clientPort, dbPort);
            byte[] data = regMsg.getBytes();

            DatagramPacket packet = new DatagramPacket(data, data.length, dirAddr, dirUdpPort);
            socket.send(packet);

            //waiting for primary response
            byte[] buf = new byte[256];

            DatagramPacket resp = new DatagramPacket(buf, buf.length);
            socket.receive(resp);

            String reply = new String(resp.getData(), 0, resp.getLength()).trim();
            DirectoryProtocol.PrimaryInfo primaryInfo = DirectoryProtocol.parsePrimary(reply);
            // Expected: PRIMARY <ip> <dbPort>
            if (primaryInfo == null) {
                System.err.println("[SERVER] Unexpected directory reply: " + reply);
                return false;
            }

            primaryDbAddr = InetAddress.getByName(primaryInfo.ip());
            primaryDbPort = primaryInfo.dbPort();

            // check if we are primary:
            // NOTE: local address matching is tricky in real env; skeleton style:
            boolean samePort = (primaryDbPort == dbPort);
            boolean sameHost = true;

            isPrimary = samePort && sameHost;

            System.out.println("[SERVER] PRIMARY is " +
                    primaryDbAddr.getHostAddress() + ":" + primaryDbPort +
                    " | isPrimary=" + isPrimary);

            return true;

        } catch (IOException e) {
            System.err.println("Error registering with directory: " + e.getMessage());
            return false;
        }
    }

    private void initOrLoadLocalDbAsPrimary() {
        try {
            if (!dbDirectory.exists() && !dbDirectory.mkdirs()) {
                System.err.println("[SERVER] Could not create db directory: " + dbDirectory.getAbsolutePath());
                return;
            }

            File dbFile = new File(dbDirectory, "quiz.db");
            dbManager = new DatabaseManager(dbFile.toPath());
            dbManager.connect();
            dbManager.initSchema();
            dbVersion = 0;
            System.out.println("[SERVER] SQLite DB ready at " + dbFile.getAbsolutePath());

        } catch (Exception e) {
            System.err.println("[SERVER] Error initializing DB: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void obtainDbFromPrimary() {
        // For now, we just behave like "ok, we got the DB"
        System.out.println("[SERVER] (DUMMY) Would fetch DB from primary " +
                primaryDbAddr.getHostAddress() + ":" + primaryDbPort);

        // Reuse the same dummy as primary so code paths are similar
        initOrLoadLocalDbAsPrimary();
    }

    private void startClientAcceptor() {
        Thread t = new Thread(() -> {
            System.out.println("Client acceptor running...");
            while (true) {
                try {
                    Socket client = clientServerSocket.accept();
                    new ClientHandler(client).start();
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
                    String hb = DirectoryProtocol.buildHeartbeat(clientPort, dbPort, dbVersion);
                    byte[] data = hb.getBytes();

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
        Thread t = new Thread(() -> {
            try (MulticastSocket mcast = new MulticastSocket(3030)) {
                InetAddress group = InetAddress.getByName("230.30.30.30");
                NetworkInterface ni = NetworkInterface.getByInetAddress(multicastIface);

                mcast.joinGroup(new InetSocketAddress(group, 3030), ni);

                byte[] buf = new byte[512];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                while (true) {
                    mcast.receive(packet);
                    String msg = new String(packet.getData(), 0, packet.getLength()).trim();
                    // Later:
                    // - ignore our own
                    // - if from PRIMARY & newer dbVersion, sync changes
                }
            } catch (IOException e) {
                System.err.println("[SERVER] Multicast listener stopped: " + e.getMessage());
            }
        }, "HeartbeatMcastListener");
        t.setDaemon(true);
        t.start();
    }

    private void handleLogin(String[] parts, BufferedReader in, PrintWriter out) {
        if (parts.length < 3) {
            out.println(ClientServerProtocol.buildLoginFail("Missing credentials"));
            return;
        }

        String email = parts[1];
        String password = parts[2];

        try {
            var user = dbManager.authenticate(email, password);
            if (user == null) {
                out.println(ClientServerProtocol.buildLoginFail("Invalid credentials"));
                return;
            }

            out.println(ClientServerProtocol.buildLoginOk(
                    user.role(),
                    "Welcome " + user.name()));

            //TODO: after login, open a post-login command loop
            postLoginLoop(user, in, out);

        } catch (Exception e) {
            e.printStackTrace();
            out.println(ClientServerProtocol.buildError("LOGIN_ERROR"));
        }
    }

    private void handleRegisterStudent(String[] parts, PrintWriter out) {
        // Example: REGISTER_STUDENT <number> <name> <email> <password>
        if (parts.length < 5) {
            out.println(ClientServerProtocol.buildRegisterFail("Missing fields"));
            return;
        }

        int number = Integer.parseInt(parts[1]);
        String name = parts[2];
        String email = parts[3];
        String password = parts[4];

        try {
            boolean ok = dbManager.registerStudent(number, name, email, password);
            if (ok)
                out.println(ClientServerProtocol.buildRegisterOk("Student registered"));
            else
                out.println(ClientServerProtocol.buildRegisterFail("Duplicate email or number"));
        } catch (Exception e) {
            out.println(ClientServerProtocol.buildRegisterFail("DB_ERROR"));
        }
    }

    private void handleRegisterTeacher(String[] parts, PrintWriter out) {
        // Example: REGISTER_TEACHER <number> <name> <email> <password>
        if (parts.length < 5) {
            out.println(ClientServerProtocol.buildRegisterFail("Missing fields"));
            return;
        }

        String name = parts[1];
        String email = parts[2];
        String password = parts[3];
        String teacherCodeHash = parts[4];

        try {
            boolean ok = dbManager.registerTeacher(name, email, password, teacherCodeHash);
            if (ok)
                out.println(ClientServerProtocol.buildRegisterOk("Teacher registered"));
            else
                out.println(ClientServerProtocol.buildRegisterFail("Duplicate email or number"));
        } catch (Exception e) {
            out.println(ClientServerProtocol.buildRegisterFail("DB_ERROR"));
        }
    }

    private void postLoginLoop(User user, BufferedReader in, PrintWriter out) throws IOException {
        String line;
        while ((line = in.readLine()) != null) {

            String[] parts = line.trim().split("\\s+");
            if (parts.length == 0)
                continue;

            String cmd = parts[0].toUpperCase();

            switch (cmd) {

                case "PING" -> out.println("PONG");

                case "LOGOUT" -> {
                    out.println("BYE");
                    return; // exit the loop and close connection
                }

                // future commands
                case "CREATE_QUESTION" -> {
                    // TODO step 5
                }

                default -> out.println(ClientServerProtocol.buildError("UNKNOWN_COMMAND"));
            }
        }
    }

    // Inner class for handling a client connection
    private class ClientHandler extends Thread {
        private final Socket socket;

        ClientHandler(Socket socket) {
            this.socket = socket;
            setName("ClientHandler-" + socket.getRemoteSocketAddress());
        }

        @Override
        public void run() {
            try (Socket s = this.socket;
                 BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                 PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {

                // Read login request
                String first = in.readLine();
                //ClientServerProtocol.LoginRequest req = ClientServerProtocol.parseLoginRequest(first);
                if (first == null) {
                    out.println(ClientServerProtocol.buildError("EMPTY_REQUEST"));
                    return;
                }

                String[] parts = first.trim().split("\\s+");
                String cmd = parts[0].toUpperCase();

                switch (cmd) {
                    case "LOGIN" -> handleLogin(parts, in, out);
                    case "REGISTER_STUDENT" -> handleRegisterStudent(parts, out);
                    case "REGISTER_TEACHER" -> handleRegisterTeacher(parts, out);
                    default -> {
                        out.println(ClientServerProtocol.buildError("UNKNOWN_COMMAND"));
                        return;
                    }
                }

                /* Post-login: simple echo loop
                String line;
                while ((line = in.readLine()) != null) {
                    // For now echo input back
                    out.println("ECHO " + line);
                } */



            } catch (IOException e) {
                System.err.println("Client handler error: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Usage: java Server <dirIP> <dirUdpPort> <dbDir> <multicastInterfaceIp>");
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
