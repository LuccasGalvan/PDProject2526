// language: java
package pt.isec.pdG36.proj.server;


import pt.isec.pdG36.proj.common.protocol.DirectoryProtocol;
import pt.isec.pdG36.proj.common.protocol.ClientServerProtocol;
import pt.isec.pdG36.proj.server.db.DatabaseManager;
import pt.isec.pdG36.proj.server.db.User;
import pt.isec.pdG36.proj.server.services.NotificationHub;
import pt.isec.pdG36.proj.server.services.StudentService;
import pt.isec.pdG36.proj.server.services.TeacherService;
import pt.isec.pdG36.proj.server.services.UserService;
import pt.isec.pdG36.proj.server.util.ServerUtil;

import java.sql.SQLException;
import java.io.*;
import java.net.*;
import java.sql.*;

public class Server {
    // protects "DB write + version bump + replication" and DB file copying
    private final Object dbLock = new Object();

    private static final String MCAST_ADDR = "230.30.30.30";
    private static final int MCAST_PORT = 3030;

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

    private UserService userService;
    private TeacherService teacherService;
    private StudentService studentService;
    private NotificationHub notificationHub;

    public Server(InetAddress dirAddr, int dirUdpPort, File dbDirectory, InetAddress multicastIface) {
        this.dirAddr = dirAddr;
        this.dirUdpPort = dirUdpPort;
        this.dbDirectory = dbDirectory;
        this.multicastIface = multicastIface;
    }

    public void start() throws Exception {
        //create TCP listening sockets on automatic ports
        clientServerSocket = new ServerSocket(0);
        dbServerSocket = new ServerSocket(0);

        int clientPort = clientServerSocket.getLocalPort();
        int dbPort = dbServerSocket.getLocalPort();

        System.out.println("Server TCP ports - clients: " + clientPort + ", dbCopy: " + dbPort);

        //register with directory
        if (!registerWithDirectory(clientPort, dbPort)) {
            System.err.println("Failed to register with directory. Exiting.");
            return;
        }

        //DB initialization / sync
        if (isPrimary) {
            initOrLoadLocalDb();
        } else {
            obtainDbFromPrimary();
        }

        //initialize services
        this.userService = new UserService(dbManager, dbLock, this);
        this.teacherService = new TeacherService(dbManager, dbLock, this);
        this.studentService = new StudentService(dbManager, dbLock, this);
        this.notificationHub = new NotificationHub();

        //start threads
        startClientAcceptor();
        startDbCopyAcceptor();
        startHeartbeatSender(clientPort, dbPort);
        startHeartbeatMulticastListener();
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

    private File chooseOrCreateDbFileForPrimary() throws IOException {
        if (!dbDirectory.exists() && !dbDirectory.mkdirs()) {
            throw new IOException("Could not create db directory: " + dbDirectory.getAbsolutePath());
        }

        File[] dbFiles = dbDirectory.listFiles((dir, name) -> name.endsWith(".db"));
        if (dbFiles == null || dbFiles.length == 0) {
            // no DBs yet -> create a new one with timestamp name
            String filename = "quiz-" + System.currentTimeMillis() + ".db";
            File f = new File(dbDirectory, filename);
            System.out.println("[SERVER] Creating new primary DB file: " + f.getAbsolutePath());
            return f;
        }

        // pick the most recently modified file
        File newest = dbFiles[0];
        for (File f : dbFiles) {
            if (f.lastModified() > newest.lastModified()) {
                newest = f;
            }
        }

        System.out.println("[SERVER] Using existing primary DB file: " + newest.getAbsolutePath());
        return newest;
    }

    private void initOrLoadLocalDb() {
        try {
            File dbFile = chooseOrCreateDbFileForPrimary();

            synchronized (dbLock) {
                dbManager = new DatabaseManager(dbFile.toPath());
                dbManager.connect();
                dbManager.initSchema();
                dbVersion = dbManager.getCurrentDbVersion();
            }

            System.out.println("[SERVER] PRIMARY SQLite DB ready at " + dbFile.getAbsolutePath()
                    + " (version = " + dbVersion + ")");

        } catch (Exception e) {
            System.err.println("[SERVER] Error initializing DB as PRIMARY: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void obtainDbFromPrimary() {
        System.out.println("[SERVER] Fetching DB from primary "
                + primaryDbAddr.getHostAddress() + ":" + primaryDbPort);

        if (!dbDirectory.exists() && !dbDirectory.mkdirs()) {
            System.err.println("[SERVER] Could not create db directory: " + dbDirectory.getAbsolutePath());
            return;
        }

        // New local file name for this copy
        File dbFile = new File(dbDirectory, "quiz-copy-" + System.currentTimeMillis() + ".db");

        try (Socket socket = new Socket(primaryDbAddr, primaryDbPort);
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             FileOutputStream fos = new FileOutputStream(dbFile)) {

            long length = in.readLong();
            byte[] buf = new byte[8192];
            long remaining = length;

            while (remaining > 0) {
                int toRead = (int) Math.min(buf.length, remaining);
                int n = in.read(buf, 0, toRead);
                if (n < 0) {
                    throw new IOException("Unexpected EOF while receiving DB file");
                }
                fos.write(buf, 0, n);
                remaining -= n;
            }

            fos.flush();

            System.out.println("[SERVER] Received DB copy into " + dbFile.getAbsolutePath()
                    + " (" + length + " bytes)");

            synchronized (dbLock) {
                dbManager = new DatabaseManager(dbFile.toPath());
                dbManager.connect();
                dbManager.initSchema();
                dbVersion = dbManager.getCurrentDbVersion();
            }

            System.out.println("[SERVER] SECONDARY SQLite DB ready at " + dbFile.getAbsolutePath()
                    + " (version = " + dbVersion + ")");

        } catch (IOException | SQLException e) {
            System.err.println("[SERVER] Error obtaining DB from primary: " + e.getMessage());
            e.printStackTrace();
            System.exit(1); //if secondary can't sync it should die
        }
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
            while (true) {
                try {
                    Socket s = dbServerSocket.accept();
                    new Thread(() -> handleDbCopyRequest(s), "DbCopyHandler-" + s.getRemoteSocketAddress()).start();
                } catch (IOException e) {
                    System.err.println("[SERVER] DB copy acceptor stopped: " + e.getMessage());
                    return;
                }
            }
        }, "DbCopyAcceptor");
        t.setDaemon(true);
        t.start();
    }

    private void handleDbCopyRequest(Socket socket) {
        try (socket;
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

            //only primary should serve DB copies
            if (!isPrimary) {
                System.out.println("[SERVER] Ignoring DB copy request on non-primary");
                return;
            }

            File dbFile = dbManager.getDbFile().toFile();

            synchronized (dbLock) {
                long length = dbFile.length();
                out.writeLong(length);

                try (FileInputStream fis = new FileInputStream(dbFile)) {
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = fis.read(buf)) > 0) {
                        out.write(buf, 0, n);
                    }
                }
                out.flush();
            }

            System.out.println("[SERVER] Sent DB copy (" + dbFile.getName() + ", " + dbFile.length() + " bytes)");

        } catch (IOException e) {
            System.err.println("[SERVER] Error handling DB copy request: " + e.getMessage());
        }
    }

    private void startHeartbeatSender(int clientPort, int dbPort) {
        Thread t = new Thread(() -> {
            try (DatagramSocket udp = new DatagramSocket()) {
                InetAddress mcastAddr = InetAddress.getByName(MCAST_ADDR);

                while (true) {
                    String hb = DirectoryProtocol.buildHeartbeat(clientPort, dbPort, dbVersion);
                    byte[] data = hb.getBytes();

                    System.out.println("[SERVER] Sending HEARTBEAT: " + hb);

                    DatagramPacket toDir = new DatagramPacket(data, data.length, dirAddr, dirUdpPort);
                    udp.send(toDir);

                    try {
                        udp.setSoTimeout(2000);
                        byte[] buf = new byte[256];
                        DatagramPacket resp = new DatagramPacket(buf, buf.length);
                        udp.receive(resp);

                        String reply = new String(resp.getData(), 0, resp.getLength()).trim();
                        DirectoryProtocol.PrimaryInfo primaryInfo = DirectoryProtocol.parsePrimary(reply);
                        if (primaryInfo != null) {
                            InetAddress newPrimaryAddr = InetAddress.getByName(primaryInfo.ip());
                            int newPrimaryDbPort = primaryInfo.dbPort();

                            boolean oldIsPrimary = this.isPrimary;

                            this.primaryDbAddr = newPrimaryAddr;
                            this.primaryDbPort = newPrimaryDbPort;

                            boolean samePort = (primaryDbPort == dbPort);
                            boolean sameHost = true;

                            this.isPrimary = samePort && sameHost;

                            if (!oldIsPrimary && this.isPrimary) {
                                System.out.println("[SERVER] PROMOTED to PRIMARY by directory heartbeat");
                            }
                        } else {
                            System.err.println("[SERVER] Unexpected reply to HEARTBEAT: " + reply);
                        }
                    } catch (SocketTimeoutException ignored) {
                        //ignore
                    }

                    // 2) send heartbeat (without SQL) to multicast group for liveness/consistency checking
                    DatagramPacket toMcast = new DatagramPacket(data, data.length, mcastAddr, MCAST_PORT);
                    udp.send(toMcast);

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

                byte[] buf = new byte[2048];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                while (true) {
                    mcast.receive(packet);
                    String msg = new String(packet.getData(), 0, packet.getLength()).trim();
                    if (msg.isEmpty()) {
                        continue;
                    }

                    DirectoryProtocol.HeartbeatInfo hb = DirectoryProtocol.parseHeartbeat(msg);
                    if (hb == null) {
                        System.err.println("[SERVER] Invalid multicast HEARTBEAT: " + msg);
                        continue;
                    }

                    InetAddress senderAddr = packet.getAddress();

                    //ignore our own heartbeats
                    boolean sameHost = senderAddr.equals(multicastIface);
                    boolean sameClientPort = hb.clientPort() == clientServerSocket.getLocalPort();
                    boolean sameDbPort = hb.dbPort() == dbServerSocket.getLocalPort();
                    if (sameHost && sameClientPort && sameDbPort) {
                        continue;
                    }

                    //primary doesnt care
                    if (isPrimary) {
                        continue;
                    }

                    //secondary only processes primary heartbeats
                    if (!senderAddr.equals(primaryDbAddr) || hb.dbPort() != primaryDbPort) {
                        continue;
                    }

                    long remoteVersion = hb.dbVersion();
                    long localVersion = this.dbVersion;

                    String sql = DirectoryProtocol.extractSqlFromHeartbeat(msg);

                    if (sql == null) {
                        // Plain heartbeat -> liveness only, no version kill
                        System.out.println("[SERVER] Plain HEARTBEAT from PRIMARY "
                                + senderAddr.getHostAddress()
                                + " remoteVersion=" + remoteVersion
                                + " localVersion=" + localVersion);
                        continue;
                    }

                    // SQL heartbeat -> replication step
                    System.out.println("[SERVER] SQL HEARTBEAT from PRIMARY "
                            + senderAddr.getHostAddress()
                            + " remoteVersion=" + remoteVersion
                            + " localVersion=" + localVersion
                            + " sql=" + sql);

                    // dbVersion check
                    if (remoteVersion != localVersion + 1) {
                        System.err.println("[SERVER] DB VERSION MISMATCH on SQL update: "
                                + "remoteVersion=" + remoteVersion
                                + " localVersion=" + localVersion
                                + " -> terminating (lost replication consistency).");
                        System.exit(1);
                    }

                    // Apply SQL on secondary
                    try (Statement st = dbManager.getConnection().createStatement()) {
                        st.executeUpdate(sql);
                        this.dbVersion = remoteVersion;
                        System.out.println("[SERVER] Applied SQL from primary. New localVersion=" + this.dbVersion);
                    } catch (SQLException e) {
                        System.err.println("[SERVER] Failed to apply SQL from primary: " + e.getMessage());
                        System.exit(1); // inconsistent -> die
                    }
                }
            } catch (IOException e) {
                System.err.println("[SERVER] Multicast listener stopped: " + e.getMessage());
            }
        }, "HeartbeatMcastListener");
        t.setDaemon(true);
        t.start();
    }

    public void sendSqlHeartbeatToSecondaries(String sql) {
        if (!isPrimary) {
            return;
        }

        try (DatagramSocket udp = new DatagramSocket()) {
            InetAddress mcastAddr = InetAddress.getByName(MCAST_ADDR);

            int clientPort = clientServerSocket.getLocalPort();
            int dbPort = dbServerSocket.getLocalPort();
            long version = this.dbVersion;

            String msg = DirectoryProtocol.buildHeartbeatWithSql(clientPort, dbPort, version, sql);
            byte[] data = msg.getBytes();

            DatagramPacket pkt = new DatagramPacket(data, data.length, mcastAddr, MCAST_PORT);
            udp.send(pkt);

            System.out.println("[SERVER] Sent SQL HEARTBEAT: " + msg);
        } catch (IOException e) {
            System.err.println("[SERVER] Failed to send SQL HEARTBEAT: " + e.getMessage());
        }
    }

    private void postLoginLoop(User user, BufferedReader in, PrintWriter out) throws IOException {
        String line;
        while ((line = in.readLine()) != null) {

            String[] parts = ServerUtil.splitCommandLine(line);
            if (parts.length == 0)
                continue;

            String cmd = parts[0].toUpperCase();

            switch (cmd) {

                //common
                case "PING" -> out.println("PONG");

                case "LOGOUT" -> {
                    out.println("BYE");
                    return; // exit the loop and close connection
                }

                case "EDIT_PROFILE" -> {
                    try {
                        userService.handleEditProfile(user, in, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                        out.println(ClientServerProtocol.buildError("EDIT_PROFILE_ERROR"));
                    }
                }

                //teacher stuff
                case "CREATE_QUESTION" -> {
                    try {
                        teacherService.handleCreateQuestion(user, in, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                        out.println(ClientServerProtocol.buildError("CREATE_QUESTION_ERROR"));
                    }
                }

                case "LIST_MY_QUESTIONS" -> {
                    if (!"TEACHER".equalsIgnoreCase(user.role())) {
                        out.println(ClientServerProtocol.buildError("ONLY_TEACHERS_CAN_LIST_QUESTIONS"));
                    } else {
                        try {
                            teacherService.handleListMyQuestions(user, parts, out);
                        } catch (Exception e) {
                            e.printStackTrace();
                            out.println(ClientServerProtocol.buildError("LIST_MY_QUESTIONS_ERROR"));
                        }
                    }
                }

                case "EDIT_QUESTION" -> {
                    if (!"TEACHER".equalsIgnoreCase(user.role())) {
                        out.println(ClientServerProtocol.buildError("NOT_A_TEACHER"));
                    } else {
                        try {
                            teacherService.handleEditQuestion(user, in, out);
                        } catch (Exception e) {
                            e.printStackTrace();
                            out.println(ClientServerProtocol.buildError("EDIT_QUESTION_ERROR"));
                        }
                    }
                }

                case "DELETE_QUESTION" -> {
                    if (!"TEACHER".equalsIgnoreCase(user.role())) {
                        out.println(ClientServerProtocol.buildError("NOT_A_TEACHER"));
                    } else {
                        try {
                            teacherService.handleDeleteQuestion(user, in, out);
                        } catch (Exception e) {
                            e.printStackTrace();
                            out.println(ClientServerProtocol.buildError("DELETE_QUESTION_ERROR"));
                        }
                    }
                }

                case "VIEW_RESULTS" -> {
                    try {
                        teacherService.handleViewResults(user, in, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                        out.println(ClientServerProtocol.buildError("VIEW_RESULTS_ERROR"));
                    }
                }

                case "EXPORT_RESULTS" -> {
                    try {
                        teacherService.handleExportResults(user, in, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                        out.println(ClientServerProtocol.buildError("EXPORT_RESULTS_ERROR"));
                    }
                }

                case "EXPORT_ALL_RESULTS" -> {
                    try {
                        teacherService.handleExportAllResults(user, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                        out.println(ClientServerProtocol.buildError("EXPORT_ALL_RESULTS_ERROR"));
                    }
                }

                //student stuff

                case "ANSWER_QUESTION" -> {
                    // only students can answer questions
                    if (!"STUDENT".equalsIgnoreCase(user.role())) {
                        out.println(ClientServerProtocol.buildError("ONLY_STUDENTS_CAN_ANSWER"));
                    } else {
                        try {
                            studentService.handleAnswerQuestion(user, in, out);
                        } catch (Exception e) {
                            e.printStackTrace();
                            out.println(ClientServerProtocol.buildError("ANSWER_QUESTION_ERROR"));
                        }
                    }
                }

                case "LIST_MY_ANSWERS" -> {
                    if (!"STUDENT".equalsIgnoreCase(user.role())) {
                        out.println(ClientServerProtocol.buildError("ONLY_STUDENTS_CAN_LIST_ANSWERS"));
                    } else {
                        try {
                            studentService.handleListMyAnswers(user, out);
                        } catch (Exception e) {
                            e.printStackTrace();
                            out.println(ClientServerProtocol.buildError("LIST_MY_ANSWERS_ERROR"));
                        }
                    }
                }
                default -> out.println(ClientServerProtocol.buildError("UNKNOWN_COMMAND"));
            }
        }
    }

    public synchronized long bumpDbVersion() throws SQLException {
        long newVersion = dbManager.incrementDbVersion();
        this.dbVersion = newVersion;
        System.out.println("[SERVER] DB version bumped to " + newVersion);
        return newVersion;
    }

    public void notifyStudents(String s) {
        notificationHub.notifyStudents(s);
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
            // try-with-resources garante fechamento de socket e streams ao sair do bloco (incl. em exceções)
            try (
                    Socket s = this.socket;
                    BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    PrintWriter out = new PrintWriter(s.getOutputStream(), true)
            ) {

                User user = null;

                while (true) {
                    String line = in.readLine();
                    if (line == null) // cliente fechou socket
                        break;

                    String[] parts = ServerUtil.splitCommandLine(line);
                    if (parts.length == 0) continue;
                    String cmd = parts[0].toUpperCase();

                    switch (cmd) {
                        //common stuff
                        case "LOGIN" -> {
                            user = userService.handleLogin(parts, in, out);
                            if (user != null) {
                                // register for notifications if this is a student
                                notificationHub.registerStudentNotification(user, out);
                                try {
                                    postLoginLoop(user, in, out);
                                } catch (IOException e) {
                                    System.err.println(getName() + " - session IO error: " + e.getMessage());
                                } finally {
                                    // remove from notification sinks and allow new login on same socket
                                    notificationHub.unregisterNotification(out);
                                    user = null;
                                }
                            }
                        }

                        //student stuff
                        case "REGISTER_STUDENT" -> userService.handleRegisterStudent(parts, out);

                        //teacher stuff
                        case "REGISTER_TEACHER" -> userService.handleRegisterTeacher(parts, out);
                        default -> out.println(ClientServerProtocol.buildError("NOT_LOGGED_IN_OR_UNKNOWN_CMD"));
                    }
                }

            } catch (IOException e) {
                System.err.println("Client handler error: " + e.getMessage());
            } finally {
                // try-with-resources já fecha socket/streams; log opcional para debug/monitorização
                System.out.println(getName() + " - connection closed and resources cleaned up.");
            }
        }
    }

    public boolean isPrimary() {
        return isPrimary;
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
