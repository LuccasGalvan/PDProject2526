// language: java
package pt.isec.pdG36.proj.server;


import pt.isec.pdG36.proj.common.protocol.DirectoryProtocol;
import pt.isec.pdG36.proj.common.protocol.ClientServerProtocol;
import pt.isec.pdG36.proj.server.db.DatabaseManager;
import pt.isec.pdG36.proj.server.db.PassUtil;
import pt.isec.pdG36.proj.server.db.User;
import java.sql.Connection;
import java.sql.SQLException;

import javax.xml.crypto.Data;
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
            initOrLoadLocalDb();
        } else {
            obtainDbFromPrimary();
        }

        // Start threads
        startClientAcceptor();
        startDbCopyAcceptor();
        startHeartbeatSender(clientPort, dbPort);
        startHeartbeatMulticastListener();

        //TODO: shutdown server option maybe?
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
            System.exit(1); // as per spec, if secondary can't sync it should die
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

            // Only primary should serve DB copies
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

                    // 1) send to directory and wait for PRIMARY reply
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
                        // directory didn’t answer this heartbeat -> ignore, try next time

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

                    if (!msg.startsWith("HEARTBEAT")) {
                        // later we can handle other message types here
                        continue;
                    }

                    DirectoryProtocol.HeartbeatInfo hb = DirectoryProtocol.parseHeartbeat(msg);
                    if (hb == null) {
                        System.err.println("[SERVER] Invalid multicast HEARTBEAT: " + msg);
                        continue;
                    }

                    InetAddress senderAddr = packet.getAddress();

                    // Ignore our own heartbeats
                    boolean sameHost = senderAddr.equals(multicastIface);
                    boolean sameClientPort = hb.clientPort() == clientServerSocket.getLocalPort();
                    boolean sameDbPort = hb.dbPort() == dbServerSocket.getLocalPort();
                    if (sameHost && sameClientPort && sameDbPort) {
                        continue;
                    }

                    // PRIMARY never consumes replication from others
                    if (isPrimary) {
                        continue;
                    }

                    // SECONDARY: only care about primary
                    if (!senderAddr.equals(primaryDbAddr) || hb.dbPort() != primaryDbPort) {
                        continue;
                    }

                    long remoteVersion = hb.dbVersion();
                    long localVersion = this.dbVersion; // volatile

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

    private void sendSqlHeartbeatToSecondaries(String sql) {
        if (!isPrimary) {
            return;
        }

        try (DatagramSocket udp = new DatagramSocket()) {
            InetAddress mcastAddr = InetAddress.getByName(MCAST_ADDR);

            int clientPort = clientServerSocket.getLocalPort();
            int dbPort = dbServerSocket.getLocalPort();
            long version = this.dbVersion; // already bumped

            String msg = DirectoryProtocol.buildHeartbeatWithSql(clientPort, dbPort, version, sql);
            byte[] data = msg.getBytes();

            DatagramPacket pkt = new DatagramPacket(data, data.length, mcastAddr, MCAST_PORT);
            udp.send(pkt);

            System.out.println("[SERVER] Sent SQL HEARTBEAT: " + msg);
        } catch (IOException e) {
            System.err.println("[SERVER] Failed to send SQL HEARTBEAT: " + e.getMessage());
        }
    }

    private User handleLogin(String[] parts, BufferedReader in, PrintWriter out) {
        // Expected: parts[0] = "LOGIN", parts[1] = email, parts[2] = password
        if (parts.length < 3) {
            out.println(ClientServerProtocol.buildLoginFail("Missing credentials"));
            return null;
        }

        String email = parts[1];
        String password = parts[2];

        try {
            User user = dbManager.authenticate(email, password);
            if (user == null) {
                out.println(ClientServerProtocol.buildLoginFail("INVALID_CREDENTIALS"));
                return null;
            }
            // Authentication OK: inform client and return authenticated user.
            out.println(ClientServerProtocol.buildLoginOk(user.role(), user.name()));
            return user;
        } catch (SQLException e) {
            e.printStackTrace();
            out.println(ClientServerProtocol.buildError("LOGIN_ERROR"));
            return null;
        }
    }

    /**
     * Split incoming line into parts. For REGISTER_STUDENT and REGISTER_TEACHER payloads,
     * fields are split by '|' (to allow spaces inside fields). For other commands, split by whitespace.
     *
     * Returned array: parts[0] = command (upper-case), subsequent indices = fields.
     */
    private String[] splitCommandLine(String line) {
        if (line == null) return new String[0];
        String trimmed = line.trim();
        if (trimmed.isEmpty()) return new String[0];

        int spaceIdx = trimmed.indexOf(' ');
        String cmd = (spaceIdx == -1) ? trimmed.toUpperCase() : trimmed.substring(0, spaceIdx).toUpperCase();

        if ("REGISTER_STUDENT".equalsIgnoreCase(cmd) || "REGISTER_TEACHER".equalsIgnoreCase(cmd)) {
            if (spaceIdx == -1) {
                // command without payload
                return new String[]{cmd};
            }
            String payload = trimmed.substring(spaceIdx + 1);
            // split preserving empty fields; trim each field
            String[] fields = payload.split("\\|", -1);
            String[] parts = new String[fields.length + 1];
            parts[0] = cmd;
            for (int i = 0; i < fields.length; i++) {
                parts[i + 1] = fields[i] == null ? "" : fields[i].trim();
            }
            return parts;
        } else {
            // Default: split by whitespace. Keep simple tokenization for other commands.
            // Use limit to preserve trailing message when relevant (e.g., LOGIN responses parsing handled elsewhere)
            return trimmed.split("\\s+");
        }
    }

    private void handleRegisterStudent(String[] parts, PrintWriter out) {
        if (!isPrimary) {
            out.println(ClientServerProtocol.buildError("Primary server is down, please retry in a few seconds"));
            return;
        }

        if (parts.length < 5) {
            out.println(ClientServerProtocol.buildRegisterFail("Missing fields"));
            return;
        }

        int number;
        try {
            number = Integer.parseInt(parts[1].trim());
        } catch (NumberFormatException e) {
            out.println(ClientServerProtocol.buildRegisterFail("INVALID_NUMBER"));
            return;
        }
        String name = parts[2];
        String email = parts[3];
        String password = parts[4];

        synchronized (dbLock) {
            try {
                DatabaseManager.RegisterResult res = dbManager.registerStudent(number, name, email, password);
                switch (res) {
                    case OK -> {
                        try {
                            long newVersion = Server.this.bumpDbVersion();

                            if (isPrimary) {
                                String passwordHash = PassUtil.hashPassword(password);

                                String sqlForReplication =
                                        "INSERT INTO users(role, student_number, name, email, password_hash) VALUES ("
                                                + "'STUDENT',"
                                                + number
                                                + ",'"
                                                + escapeSqlLiteral(name)
                                                + "','"
                                                + escapeSqlLiteral(email)
                                                + "','"
                                                + escapeSqlLiteral(passwordHash)
                                                + "')";

                                sendSqlHeartbeatToSecondaries(sqlForReplication);
                            }

                            out.println(ClientServerProtocol.buildRegisterOk("Student registered"));
                        } catch (SQLException e) {
                            System.err.println("[SERVER] Failed to bump DB version / send SQL heartbeat after REGISTER_STUDENT: "
                                    + e.getMessage());
                            out.println(ClientServerProtocol.buildError("DB_ERROR"));
                        }
                    }
                    case EMAIL_IN_USE -> out.println(ClientServerProtocol.buildRegisterFail("EMAIL_IN_USE"));
                    case NUMBER_IN_USE -> out.println(ClientServerProtocol.buildRegisterFail("NUMBER_IN_USE"));
                    default -> out.println(ClientServerProtocol.buildRegisterFail("DB_ERROR"));
                }
            } catch (Exception e) {
                out.println(ClientServerProtocol.buildError("DB_ERROR"));
            }
        }
    }

    private void handleRegisterTeacher(String[] parts, PrintWriter out) {
        if (!isPrimary) {
            out.println(ClientServerProtocol.buildError("Primary server is down, please retry in a few seconds"));
            return;
        }

        // Example: REGISTER_TEACHER <name>|<email>|<password>|<teacherCode>
        if (parts.length < 5) {
            out.println(ClientServerProtocol.buildRegisterFail("Missing fields"));
            return;
        }

        String name = parts[1];
        String email = parts[2];
        String password = parts[3];
        String teacherCode = parts[4];

        synchronized (dbLock) {
            try {
                DatabaseManager.RegisterResult res = dbManager.registerTeacher(name, email, password, teacherCode);
                switch (res) {
                    case OK -> {
                        try {
                            long newVersion = Server.this.bumpDbVersion();

                            if (isPrimary) {
                                String passwordHash = PassUtil.hashPassword(password);

                                String sqlForReplication =
                                        "INSERT INTO users(role, student_number, name, email, password_hash) VALUES ("
                                                + "'TEACHER',"
                                                + "NULL,"
                                                + "'"
                                                + escapeSqlLiteral(name)
                                                + "','"
                                                + escapeSqlLiteral(email)
                                                + "','"
                                                + escapeSqlLiteral(passwordHash)
                                                + "')";

                                sendSqlHeartbeatToSecondaries(sqlForReplication);
                            }

                            out.println(ClientServerProtocol.buildRegisterOk("Teacher registered"));
                        } catch (SQLException e) {
                            System.err.println("[SERVER] Failed to bump DB version / send SQL heartbeat after REGISTER_TEACHER: "
                                    + e.getMessage());
                            out.println(ClientServerProtocol.buildError("DB_ERROR"));
                        }
                    }
                    case EMAIL_IN_USE -> out.println(ClientServerProtocol.buildRegisterFail("EMAIL_IN_USE"));
                    case NUMBER_IN_USE -> out.println(ClientServerProtocol.buildRegisterFail("NUMBER_IN_USE"));
                    case DatabaseManager.RegisterResult.INVALID_TEACHER_CODE ->
                            out.println(ClientServerProtocol.buildRegisterFail("INVALID_TEACHER_CODE"));
                    default -> out.println(ClientServerProtocol.buildRegisterFail("DB_ERROR"));
                }
            } catch (Exception e) {
                out.println(ClientServerProtocol.buildError("DB_ERROR"));
            }
        }
    }

    private void postLoginLoop(User user, BufferedReader in, PrintWriter out) throws IOException {
        String line;
        while ((line = in.readLine()) != null) {

            String[] parts = splitCommandLine(line);
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
                        handleEditProfile(user, in, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                        out.println(ClientServerProtocol.buildError("EDIT_PROFILE_ERROR"));
                    }
                }

                //teacher stuff
                case "CREATE_QUESTION" -> {
                    try {
                        handleCreateQuestion(user, in, out);
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
                            handleListMyQuestions(user, parts, out);
                        } catch (Exception e) {
                            e.printStackTrace();
                            out.println(ClientServerProtocol.buildError("LIST_MY_QUESTIONS_ERROR"));
                        }
                    }
                }

                case "VIEW_RESULTS" -> {
                    try {
                        handleViewResults(user, in, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                        out.println(ClientServerProtocol.buildError("VIEW_RESULTS_ERROR"));
                    }
                }

                case "EXPORT_RESULTS" -> {
                    try {
                        handleExportResults(user, in, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                        out.println(ClientServerProtocol.buildError("EXPORT_RESULTS_ERROR"));
                    }
                }

                case "EXPORT_ALL_RESULTS" -> {
                    try {
                        handleExportAllResults(user, out);
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
                            handleAnswerQuestion(user, in, out);
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
                            handleListMyAnswers(user, out);
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

    private void handleEditProfile(User user, BufferedReader in, PrintWriter out) throws IOException {
        if (!isPrimary) {
            out.println(ClientServerProtocol.buildError("Primary server is down, please retry in a few seconds"));
            return;
        }

        // Ask for new values; blank = keep current
        out.println("PROMPT New name (leave blank to keep current: " + user.name() + "):");
        String newName = in.readLine();
        if (newName == null) {
            out.println(ClientServerProtocol.buildError("EDIT_PROFILE_CANCELLED"));
            return;
        }
        newName = newName.trim();

        out.println("PROMPT New email (leave blank to keep current):");
        String newEmail = in.readLine();
        if (newEmail == null) {
            out.println(ClientServerProtocol.buildError("EDIT_PROFILE_CANCELLED"));
            return;
        }
        newEmail = newEmail.trim();

        out.println("PROMPT New password (leave blank to keep current):");
        String newPassword = in.readLine();
        if (newPassword == null) {
            out.println(ClientServerProtocol.buildError("EDIT_PROFILE_CANCELLED"));
            return;
        }
        newPassword = newPassword.trim();

        // avoid '|' which breaks our protocol
        if ((newName != null && newName.contains("|"))
                || (newEmail != null && newEmail.contains("|"))
                || (newPassword != null && newPassword.contains("|"))) {
            out.println(ClientServerProtocol.buildError("Fields cannot contain the '|' character"));
            return;
        }

        synchronized (dbLock) {
            Connection conn = null;
            String finalName = null;
            String finalEmail = null;
            String finalHash = null;

            try {
                conn = dbManager.getConnection();

                // 1) load current values
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT name, email, password_hash FROM users WHERE id = ?")) {
                    ps.setLong(1, user.id());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            out.println(ClientServerProtocol.buildError("USER_NOT_FOUND"));
                            return;
                        }
                        String currentName = rs.getString("name");
                        String currentEmail = rs.getString("email");
                        String currentHash = rs.getString("password_hash");

                        finalName = (newName == null || newName.isBlank()) ? currentName : newName;
                        finalEmail = (newEmail == null || newEmail.isBlank()) ? currentEmail : newEmail;
                        finalHash = (newPassword == null || newPassword.isBlank())
                                ? currentHash
                                : PassUtil.hashPassword(newPassword);
                    }
                }

                // 2) apply update (only name, email, password_hash)
                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE users SET name = ?, email = ?, password_hash = ? WHERE id = ?")) {
                    ps.setString(1, finalName);
                    ps.setString(2, finalEmail);
                    ps.setString(3, finalHash);
                    ps.setLong(4, user.id());
                    ps.executeUpdate();
                }

                // 3) bump version and replicate UPDATE to secondaries
                try {
                    long newVersion = bumpDbVersion();
                    if (isPrimary) {
                        String sql = "UPDATE users SET "
                                + "name='" + escapeSqlLiteral(finalName) + "', "
                                + "email='" + escapeSqlLiteral(finalEmail) + "', "
                                + "password_hash='" + escapeSqlLiteral(finalHash) + "' "
                                + "WHERE id=" + user.id();
                        sendSqlHeartbeatToSecondaries(sql);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    out.println(ClientServerProtocol.buildError("EDIT_PROFILE_VERSION_ERROR"));
                    return;
                }

                out.println("BLOCK Profile updated successfully.");

            } catch (SQLException e) {
                e.printStackTrace();
                String msg = e.getMessage();
                if (msg != null && (msg.contains("email") || msg.contains("users.email"))) {
                    out.println(ClientServerProtocol.buildError("EMAIL_IN_USE"));
                } else {
                    out.println(ClientServerProtocol.buildError("EDIT_PROFILE_DB_ERROR"));
                }
            }
        }
    }

    private String generateAccessCode() {
        //TODO make sure theres no equal accesCodes in DB just in case
        String chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
        java.util.Random rnd = new java.util.Random();
        StringBuilder sb = new StringBuilder(6);
        for (int i = 0; i < 6; i++) {
            sb.append(chars.charAt(rnd.nextInt(chars.length())));
        }
        return sb.toString();
    }

    private void handleListMyAnswers(User user, PrintWriter out) throws SQLException {
        synchronized (dbLock) {
            java.util.List<DatabaseManager.StudentAnswerDTO> answers =
                    dbManager.findClosedAnswersForStudent(user.id());

            StringBuilder sb = new StringBuilder();
            if (answers.isEmpty()) {
                sb.append("== You have no answered questions whose answering time has expired ==\n");
            } else {
                sb.append("== Your answered questions (closed) ==\n");
                int idx = 1;
                for (DatabaseManager.StudentAnswerDTO a : answers) {
                    // compute state string just for info
                    java.time.LocalDateTime start = java.time.LocalDateTime.parse(a.startTime());
                    java.time.LocalDateTime end = java.time.LocalDateTime.parse(a.endTime());

                    sb.append(idx++).append(") [Q").append(a.questionId()).append("] ")
                            .append(a.statement()).append("\n")
                            .append("   Start: ").append(start).append(" | End: ").append(end).append("\n")
                            .append("   Your answer: ").append(a.optionCode())
                            .append(" -> ").append(a.correct() ? "CORRECT" : "WRONG")
                            .append("\n\n");
                }
            }

            String payload = sb.toString().replace("\r", "").replace("\n", "\\n");
            out.println("BLOCK " + payload);
        }
    }

    private void handleListMyQuestions(User user, String[] parts, PrintWriter out) throws SQLException {
        String filter = "ALL";
        if (parts.length >= 2) {
            filter = parts[1].toUpperCase(); // SCHEDULED | ONGOING | CLOSED | ALL
        }

        java.time.LocalDateTime now = java.time.LocalDateTime.now();

        synchronized (dbLock) {
            java.util.List<DatabaseManager.QuestionDTO> questions =
                    dbManager.findQuestionsByTeacher(user.id());

            StringBuilder sb = new StringBuilder();
            if (questions.isEmpty()) {
                sb.append("== You have not created any questions yet ==\n");
            } else {
                sb.append("== Your questions ==\n");
                int idx = 1;
                for (DatabaseManager.QuestionDTO q : questions) {
                    java.time.LocalDateTime start = java.time.LocalDateTime.parse(q.startTime());
                    java.time.LocalDateTime end = java.time.LocalDateTime.parse(q.endTime());

                    String state;
                    if (now.isBefore(start)) {
                        state = "SCHEDULED";
                    } else if (now.isAfter(end)) {
                        state = "CLOSED";
                    } else {
                        state = "ONGOING";
                    }

                    // apply filter
                    if (!"ALL".equals(filter) && !state.equalsIgnoreCase(filter)) {
                        continue;
                    }

                    sb.append(idx++).append(") [Q").append(q.id()).append("] ")
                            .append(q.statement()).append("\n")
                            .append("   State: ").append(state)
                            .append(" | Start: ").append(start)
                            .append(" | End: ").append(end).append("\n")
                            .append("   Access code: ").append(q.accessCode())
                            .append("\n\n");
                }

                if (sb.toString().endsWith("== Your questions ==\n")) {
                    sb.append("No questions match the given filter.\n");
                }
            }

            String payload = sb.toString().replace("\r", "").replace("\n", "\\n");
            out.println("BLOCK " + payload);
        }
    }

    private void handleCreateQuestion(User user, BufferedReader in, PrintWriter out) throws IOException, SQLException {
        if (!isPrimary) {
            out.println(ClientServerProtocol.buildError("Primary server is down, please retry in a few seconds"));
            return;
        }

        // Only teachers can create questions
        if (!"TEACHER".equalsIgnoreCase(user.role())) {
            out.println(ClientServerProtocol.buildError("NOT_A_TEACHER"));
            return;
        }

        // 1) Ask for statement
        out.println("Enter question statement:");
        String statement = in.readLine();
        if (statement == null || statement.isBlank()) {
            out.println(ClientServerProtocol.buildError("EMPTY_STATEMENT"));
            return;
        }

        // 2) Ask start/end time as simple strings for now
        out.println("Enter start time (e.g., 2025-01-01T10:00):");
        String startTime = in.readLine();
        out.println("Enter end time (e.g., 2025-01-01T10:10):");
        String endTime = in.readLine();

        // 3) Ask number of options
        int numOptions = 0;
        while (true) {
            out.println("Enter number of options (>= 2):");
            String line = in.readLine();
            if (line == null) {
                out.println(ClientServerProtocol.buildError("ABORTED"));
                return;
            }
            try {
                numOptions = Integer.parseInt(line.trim());
                if (numOptions >= 2)
                    break;
                out.println("Must be >= 2.");
            } catch (NumberFormatException e) {
                out.println("Invalid number, try again.");
            }
        }

        // 4) Gather options
        String[] codes = new String[numOptions];
        String[] texts = new String[numOptions];
        boolean[] correctFlags = new boolean[numOptions];
        boolean isCorrect = false;

        for (int i = 0; i < numOptions; i++) {
            out.println("Option " + (i + 1) + " - code (e.g., A, B, C):");
            codes[i] = in.readLine();

            out.println("Option " + (i + 1) + " - text:");
            texts[i] = in.readLine();

            if(isCorrect == false){
                out.println("Is this the correct option? (yes/no):");
                String ans = in.readLine();
                correctFlags[i] = ans != null && ans.trim().equalsIgnoreCase("yes");
                isCorrect = true;
            }
        }

        // Ensure at least one correct option
        boolean anyCorrect = false;
        for (boolean b : correctFlags) {
            if (b) { anyCorrect = true; break; }
        }
        if (!anyCorrect) {
            out.println(ClientServerProtocol.buildError("NO_CORRECT_OPTION"));
            return;
        }

        // 5) Generate access code
        String accessCode = generateAccessCode();

        // 6) Store in DB with a transaction
        synchronized (dbLock) {
            Connection conn = dbManager.getConnection();
            boolean oldAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                long qId = dbManager.insertQuestion(
                        user.id(),       // teacherId
                        statement,
                        startTime,
                        endTime,
                        accessCode
                );

                for (int i = 0; i < numOptions; i++) {
                    dbManager.insertOption(qId, codes[i], texts[i], correctFlags[i]);
                }

                conn.commit();

                try {
                    if (isPrimary) {
                        // 1) replicate question INSERT (with explicit id)
                        {
                            long newVersion = Server.this.bumpDbVersion();

                            String sqlQuestion =
                                    "INSERT INTO questions(id, teacherId, statement, startTime, endTime, accessCode) VALUES ("
                                            + qId + ","
                                            + user.id()
                                            + ",'"
                                            + escapeSqlLiteral(statement)
                                            + "','"
                                            + escapeSqlLiteral(startTime)
                                            + "','"
                                            + escapeSqlLiteral(endTime)
                                            + "','"
                                            + escapeSqlLiteral(accessCode)
                                            + "')";

                            sendSqlHeartbeatToSecondaries(sqlQuestion);
                        }

                        // 2) replicate each option INSERT
                        for (int i = 0; i < numOptions; i++) {
                            long newVersion = Server.this.bumpDbVersion();

                            String sqlOption =
                                    "INSERT INTO options(questionId, code, text, isCorrect) VALUES ("
                                            + qId
                                            + ",'"
                                            + escapeSqlLiteral(codes[i])
                                            + "','"
                                            + escapeSqlLiteral(texts[i])
                                            + "',"
                                            + (correctFlags[i] ? 1 : 0)
                                            + ")";

                            sendSqlHeartbeatToSecondaries(sqlOption);
                        }
                    }

                    out.println("CREATE_QUESTION_OK Access code: " + accessCode);
                } catch (SQLException e) {
                    System.err.println("[SERVER] Failed to bump DB version / send SQL heartbeat after CREATE_QUESTION: "
                            + e.getMessage());
                    out.println(ClientServerProtocol.buildError("DB_ERROR"));
                }

            } catch (SQLException e) {
                conn.rollback();
                e.printStackTrace();
                out.println(ClientServerProtocol.buildError("DB_ERROR_CREATING_QUESTION"));

            } finally {
                conn.setAutoCommit(oldAutoCommit);
            }
        }
    }

    //will show results of a specific question
    private void handleViewResults(User user, BufferedReader in, PrintWriter out) throws IOException {
        // 1) Only teachers
        if (!"TEACHER".equalsIgnoreCase(user.role())) {
            out.println(ClientServerProtocol.buildError("NOT_A_TEACHER"));
            return;
        }

        // 2) Ask for access code
        out.println("PROMPT Access code of question:");
        String accessCode = in.readLine();
        if (accessCode == null || accessCode.isBlank()) {
            out.println(ClientServerProtocol.buildError("MISSING_ACCESS_CODE"));
            return;
        }
        accessCode = accessCode.trim();

        synchronized (dbLock) {
            try {
                // 3) Load question by access code
                DatabaseManager.QuestionDTO q = dbManager.findQuestionByAccessCode(accessCode);
                if (q == null) {
                    out.println(ClientServerProtocol.buildError("QUESTION_NOT_FOUND"));
                    return;
                }

                // 4) Ensure this teacher owns the question
                if (q.teacherId() != user.id()) {
                    out.println(ClientServerProtocol.buildError("NOT_YOUR_QUESTION"));
                    return;
                }

                // 5) Ensure the question is expired
                java.time.LocalDateTime now = java.time.LocalDateTime.now();
                java.time.LocalDateTime end = java.time.LocalDateTime.parse(q.endTime());
                if (now.isBefore(end)) {
                    out.println(ClientServerProtocol.buildError("QUESTION_NOT_EXPIRED"));
                    return;
                }

                // 6) Load options and answers
                java.util.List<DatabaseManager.OptionDTO> opts =
                        dbManager.findOptionsForQuestion(q.id());

                java.util.List<DatabaseManager.AnswerResultRow> answers =
                        dbManager.findAnswersForQuestion(q.id());

                // 7) Build the report text
                StringBuilder sb = new StringBuilder();

                sb.append("Results for question ").append(q.accessCode()).append("\n");
                sb.append("Statement: ").append(q.statement()).append("\n");
                sb.append("Start: ").append(q.startTime()).append("\n");
                sb.append("End: ").append(q.endTime()).append("\n");
                sb.append("\nOptions:\n");

                for (DatabaseManager.OptionDTO o : opts) {
                    sb.append("  ").append(o.code()).append(") ").append(o.text());
                    if (o.isCorrect()) {
                        sb.append("  [CORRECT]");
                    }
                    sb.append("\n");
                }

                sb.append("\nSubmitted answers:\n");
                if (answers.isEmpty()) {
                    sb.append("  (no answers submitted)\n");
                } else {
                    for (DatabaseManager.AnswerResultRow row : answers) {
                        sb.append("  [").append(row.timestamp()).append("] ");

                        if (row.studentNumber() != null) {
                            sb.append("#").append(row.studentNumber()).append(" ");
                        }

                        sb.append(row.studentName())
                                .append(" <").append(row.studentEmail()).append(">")
                                .append(" -> ").append(row.optionCode());

                        if (row.correct()) {
                            sb.append(" (CORRECT)");
                        } else {
                            sb.append(" (WRONG)");
                        }
                        sb.append("\n");
                    }
                }

                // 8) Encode as RESULT_BLOCK
                String payload = sb.toString()
                        .replace("\r", "")
                        .replace("\n", "\\n");

                out.println("RESULT_BLOCK " + payload);

            } catch (SQLException e) {
                e.printStackTrace();
                out.println(ClientServerProtocol.buildError("VIEW_RESULTS_DB_ERROR"));
            }
        }
    }

    private void handleExportResults(User user, BufferedReader in, PrintWriter out) throws IOException {
        // only teachers
        if (!"TEACHER".equalsIgnoreCase(user.role())) {
            out.println(ClientServerProtocol.buildError("NOT_A_TEACHER"));
            return;
        }

        // ask access code
        out.println("PROMPT Access code of question to export:");
        String accessCode = in.readLine();
        if (accessCode == null || accessCode.isBlank()) {
            out.println(ClientServerProtocol.buildError("MISSING_ACCESS_CODE"));
            return;
        }
        accessCode = accessCode.trim();

        DatabaseManager.QuestionDTO q;
        java.util.List<DatabaseManager.OptionDTO> opts;
        java.util.List<DatabaseManager.AnswerResultRow> answers;

        // read from DB under lock
        synchronized (dbLock) {
            try {
                q = dbManager.findQuestionByAccessCode(accessCode);
                if (q == null) {
                    out.println(ClientServerProtocol.buildError("QUESTION_NOT_FOUND"));
                    return;
                }

                // must belong to this teacher
                if (q.teacherId() != user.id()) {
                    out.println(ClientServerProtocol.buildError("NOT_OWNER_OF_QUESTION"));
                    return;
                }

                // only closed questions can be exported
                java.time.LocalDateTime end = java.time.LocalDateTime.parse(q.endTime());
                java.time.LocalDateTime now = java.time.LocalDateTime.now();
                if (!now.isAfter(end)) {
                    out.println(ClientServerProtocol.buildError("QUESTION_NOT_CLOSED"));
                    return;
                }

                opts = dbManager.findOptionsForQuestion(q.id());
                answers = dbManager.findAnswersForQuestion(q.id());

            } catch (SQLException e) {
                e.printStackTrace();
                out.println(ClientServerProtocol.buildError("EXPORT_RESULTS_DB_ERROR"));
                return;
            }
        }

        File exportDir = getGlobalExportsDir();

        String safeCode = accessCode.replaceAll("[^a-zA-Z0-9_-]", "_");
        File csvFile = new File(
                exportDir,
                "question-" + q.id() + "-" + safeCode + "-" + System.currentTimeMillis() + ".csv"
        );

        try (PrintWriter pw = new PrintWriter(csvFile)) {
            writeQuestionResultsToCsv(pw, q, opts, answers);
        } catch (IOException e) {
            e.printStackTrace();
            out.println(ClientServerProtocol.buildError("EXPORT_RESULTS_IO_ERROR"));
            return;
        }

        out.println("EXPORT_OK " + csvFile.getAbsolutePath());
    }

    private void handleExportAllResults(User user, PrintWriter out) {
        if (!"TEACHER".equalsIgnoreCase(user.role())) {
            out.println(ClientServerProtocol.buildError("NOT_A_TEACHER"));
            return;
        }

        java.time.LocalDateTime now = java.time.LocalDateTime.now();
        java.util.List<QuestionExportData> toExport = new java.util.ArrayList<>();

        // load all closed questions + their data under lock
        synchronized (dbLock) {
            try {
                java.util.List<DatabaseManager.QuestionDTO> questions =
                        dbManager.findQuestionsByTeacher(user.id());

                for (DatabaseManager.QuestionDTO q : questions) {
                    java.time.LocalDateTime end = java.time.LocalDateTime.parse(q.endTime());
                    if (!now.isAfter(end)) {
                        // not closed yet
                        continue;
                    }

                    java.util.List<DatabaseManager.OptionDTO> opts =
                            dbManager.findOptionsForQuestion(q.id());
                    java.util.List<DatabaseManager.AnswerResultRow> answers =
                            dbManager.findAnswersForQuestion(q.id());

                    toExport.add(new QuestionExportData(q, opts, answers));
                }

            } catch (SQLException e) {
                e.printStackTrace();
                out.println(ClientServerProtocol.buildError("EXPORT_ALL_RESULTS_DB_ERROR"));
                return;
            }
        }

        if (toExport.isEmpty()) {
            out.println(ClientServerProtocol.buildError("NO_CLOSED_QUESTIONS_TO_EXPORT"));
            return;
        }

        File exportDir = getGlobalExportsDir();

        File csvFile = new File(
                exportDir,
                "all-questions-teacher-" + user.email() + "-" + System.currentTimeMillis() + ".csv"
        );

        try (PrintWriter pw = new PrintWriter(csvFile)) {
            boolean first = true;
            for (QuestionExportData qd : toExport) {
                if (!first) {
                    pw.println();
                    pw.println();
                }
                first = false;

                writeQuestionResultsToCsv(
                        pw,
                        qd.question(),
                        qd.options(),
                        qd.answers()
                );
            }
        } catch (IOException e) {
            e.printStackTrace();
            out.println(ClientServerProtocol.buildError("EXPORT_ALL_RESULTS_IO_ERROR"));
            return;
        }

        out.println("EXPORT_OK " + csvFile.getAbsolutePath());
    }

    private void handleAnswerQuestion(User user, BufferedReader in, PrintWriter out) throws IOException {
        if (!isPrimary) {
            out.println(ClientServerProtocol.buildError("Primary server is down, please retry in a few seconds"));
            return;
        }

        if (!"STUDENT".equalsIgnoreCase(user.role())) {
            out.println(ClientServerProtocol.buildError("ONLY_STUDENTS_CAN_ANSWER"));
            return;
        }

        out.println("PROMPT Access code:");
        String accessCode = in.readLine();
        if (accessCode == null || accessCode.isBlank()) {
            out.println(ClientServerProtocol.buildError("MISSING_ACCESS_CODE"));
            return;
        }
        accessCode = accessCode.trim();

        // Variáveis locais para ler do DB dentro do lock e depois liberar
        long questionId;
        String statement;
        java.time.LocalDateTime start;
        java.time.LocalDateTime end;
        java.util.List<String[]> options = new java.util.ArrayList<>(); // [code, text]

        // Ler tudo do BD dentro do lock
        synchronized (dbLock) {
            try {
                Connection conn = dbManager.getConnection();

                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT id, statement, startTime, endTime FROM questions WHERE accessCode = ?")) {
                    ps.setString(1, accessCode);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            out.println(ClientServerProtocol.buildError("QUESTION_NOT_FOUND"));
                            return;
                        }
                        questionId = rs.getLong("id");
                        statement = rs.getString("statement");
                        start = java.time.LocalDateTime.parse(rs.getString("startTime"));
                        end = java.time.LocalDateTime.parse(rs.getString("endTime"));
                    }
                }

                java.time.LocalDateTime now = java.time.LocalDateTime.now();
                if (now.isBefore(start) || now.isAfter(end)) {
                    out.println(ClientServerProtocol.buildError("QUESTION_NOT_ACTIVE"));
                    return;
                }

                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT COUNT(*) FROM answers WHERE studentId = ? AND questionId = ?")) {
                    ps.setLong(1, user.id());
                    ps.setLong(2, questionId);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next() && rs.getInt(1) > 0) {
                            out.println(ClientServerProtocol.buildError("ALREADY_ANSWERED"));
                            return;
                        }
                    }
                }

                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT code, text FROM options WHERE questionId = ? ORDER BY code")) {
                    ps.setLong(1, questionId);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            options.add(new String[]{ rs.getString("code"), rs.getString("text") });
                        }
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
                out.println(ClientServerProtocol.buildError("DB_ERROR_ANSWERING"));
                return;
            }
        } // fim do synchronized - lock liberado aqui

        // Construir e enviar QUESTION_BLOCK fora do lock
        StringBuilder sb = new StringBuilder();
        sb.append("Question: ").append(statement).append("\n");
        sb.append("Options:").append("\n");
        for (String[] opt : options) {
            sb.append(opt[0]).append(") ").append(opt[1]).append("\n");
        }
        String questionText = sb.toString().replace("\r", "").replace("\n", "\\n");
        out.println("QUESTION_BLOCK " + questionText);

        // Ler a resposta do cliente sem segurar o lock
        String optionCode = in.readLine();
        if (optionCode == null || optionCode.isBlank()) {
            out.println(ClientServerProtocol.buildError("MISSING_OPTION_CODE"));
            out.println("QUESTION_ABORTED");
            return;
        }
        optionCode = optionCode.trim();

        // Reentrar no lock apenas para validar e gravar
        synchronized (dbLock) {
            try {
                Connection conn = dbManager.getConnection();

                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT COUNT(*) FROM options WHERE questionId = ? AND code = ?")) {
                    ps.setLong(1, questionId);
                    ps.setString(2, optionCode);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next() || rs.getInt(1) == 0) {
                            out.println(ClientServerProtocol.buildError("INVALID_OPTION_CODE"));
                            out.println("QUESTION_ABORTED");
                            return;
                        }
                    }
                }

                String timestamp = java.time.LocalDateTime.now().toString();

                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO answers(studentId, questionId, optionCode, timestamp) VALUES (?,?,?,?)")) {
                    ps.setLong(1, user.id());
                    ps.setLong(2, questionId);
                    ps.setString(3, optionCode);
                    ps.setString(4, timestamp);
                    ps.executeUpdate();
                }

                try {
                    long newVersion = Server.this.bumpDbVersion();
                    if (isPrimary) {
                        String sqlForReplication =
                                "INSERT INTO answers(studentId, questionId, optionCode, timestamp) VALUES ("
                                        + user.id()
                                        + ","
                                        + questionId
                                        + ",'"
                                        + escapeSqlLiteral(optionCode)
                                        + "','"
                                        + escapeSqlLiteral(timestamp)
                                        + "')";
                        sendSqlHeartbeatToSecondaries(sqlForReplication);
                    }
                } catch (SQLException e) {
                    System.err.println("[SERVER] Failed to bump DB version / send SQL heartbeat after ANSWER_QUESTION: "
                            + e.getMessage());
                    out.println(ClientServerProtocol.buildError("DB_ERROR"));
                    return;
                }

                out.println("ANSWER_OK");
            } catch (SQLException e) {
                e.printStackTrace();
                out.println(ClientServerProtocol.buildError("DB_ERROR_ANSWERING"));
                out.println("QUESTION_ABORTED");
            }
        }
    }

    public synchronized long bumpDbVersion() throws SQLException {
        long newVersion = dbManager.incrementDbVersion();
        this.dbVersion = newVersion;
        System.out.println("[SERVER] DB version bumped to " + newVersion);
        return newVersion;
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

                    String[] parts = splitCommandLine(line);
                    if (parts.length == 0) continue;
                    String cmd = parts[0].toUpperCase();

                    switch (cmd) {
                        //common stuff
                        case "LOGIN" -> {
                            // handleLogin autentica e retorna o User em caso de sucesso.
                            // Se obtivermos um User, entramos no loop de sessão (postLoginLoop)
                            user = handleLogin(parts, in, out);
                            if (user != null) {
                                try {
                                    postLoginLoop(user, in, out);
                                } catch (IOException e) {
                                    System.err.println(getName() + " - session IO error: " + e.getMessage());
                                } finally {
                                    // sessão terminada (logout ou erro) -> user volta a null (permitir novo login na mesma conexão)
                                    user = null;
                                }
                            }
                        }

                        //student stuff
                        case "REGISTER_STUDENT" -> handleRegisterStudent(parts, out);

                        //teacher stuff
                        case "REGISTER_TEACHER" -> handleRegisterTeacher(parts, out);
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

    private static String escapeSqlLiteral(String value) {
        if (value == null) return "";
        return value.replace("'", "''");
    }

    // Helper to encode a value as a CSV field (always quoted)
    private static String toCsvField(String value) {
        if (value == null)
            return "\"\"";
        String v = value.replace("\"", "\"\""); // escape double-quotes
        return "\"" + v + "\"";
    }

    // Small record to hold all info for a question export
    private record QuestionExportData(
            DatabaseManager.QuestionDTO question,
            java.util.List<DatabaseManager.OptionDTO> options,
            java.util.List<DatabaseManager.AnswerResultRow> answers
    ) {}

    // Writes ONE question (metadata + options + answers) in CSV format to pw
    private void writeQuestionResultsToCsv(
            PrintWriter pw,
            DatabaseManager.QuestionDTO q,
            java.util.List<DatabaseManager.OptionDTO> opts,
            java.util.List<DatabaseManager.AnswerResultRow> answers
    ) {
        // Question header
        pw.println("QuestionID,AccessCode,Statement,StartTime,EndTime");
        pw.println(
                toCsvField(String.valueOf(q.id())) + "," +
                        toCsvField(q.accessCode()) + "," +
                        toCsvField(q.statement()) + "," +
                        toCsvField(q.startTime()) + "," +
                        toCsvField(q.endTime())
        );

        pw.println();
        pw.println("OptionCode,OptionText,IsCorrect");
        for (DatabaseManager.OptionDTO o : opts) {
            pw.println(
                    toCsvField(o.code()) + "," +
                            toCsvField(o.text()) + "," +
                            toCsvField(o.isCorrect() ? "true" : "false")
            );
        }

        pw.println();
        pw.println("Timestamp,StudentNumber,StudentName,StudentEmail,OptionCode,Correct");
        for (DatabaseManager.AnswerResultRow row : answers) {
            String studentNumberStr = (row.studentNumber() != null)
                    ? String.valueOf(row.studentNumber())
                    : "";
            pw.println(
                    toCsvField(row.timestamp()) + "," +
                            toCsvField(studentNumberStr) + "," +
                            toCsvField(row.studentName()) + "," +
                            toCsvField(row.studentEmail()) + "," +
                            toCsvField(row.optionCode()) + "," +
                            toCsvField(row.correct() ? "true" : "false")
            );
        }
    }

    private static File getGlobalExportsDir() {
        File root = new File(System.getProperty("user.dir")); // project root at runtime
        File exportsDir = new File(root, "exports");
        if (!exportsDir.exists()) {
            exportsDir.mkdirs();
        }
        return exportsDir;
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
