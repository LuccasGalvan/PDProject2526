// language: java
package pt.isec.pdG36.proj.server;


import pt.isec.pdG36.proj.common.protocol.DirectoryProtocol;
import pt.isec.pdG36.proj.common.protocol.ClientServerProtocol;
import pt.isec.pdG36.proj.server.db.DatabaseManager;
import pt.isec.pdG36.proj.server.db.User;
import java.sql.Connection;
import java.sql.SQLException;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.*;
import java.sql.*;

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

    private User handleLogin(String[] parts, BufferedReader in, PrintWriter out) {
        if (parts.length < 3) {
            out.println(ClientServerProtocol.buildLoginFail("Missing credentials"));
            return null;
        }

        String email = parts[1];
        String password = parts[2];

        try {
            User user = dbManager.authenticate(email, password);
            if (user == null) {
                out.println(ClientServerProtocol.buildLoginFail("Invalid credentials"));
                return null;
            }

            out.println(ClientServerProtocol.buildLoginOk(
                    user.role(),
                    "Welcome " + user.name()));

            //after login, open a post-login command loop
            postLoginLoop(user, in, out);

            return null;

        } catch (Exception e) {
            e.printStackTrace();
            out.println(ClientServerProtocol.buildError("LOGIN_ERROR"));
            return null;
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
                    try {
                        handleCreateQuestion(user, in, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                        out.println(ClientServerProtocol.buildError("CREATE_QUESTION_ERROR"));
                    }
                }

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

                default -> out.println(ClientServerProtocol.buildError("UNKNOWN_COMMAND"));
            }
        }
    }

    private String generateAccessCode() {
        String chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // avoid confusing chars like 0/O, 1/I
        java.util.Random rnd = new java.util.Random();
        StringBuilder sb = new StringBuilder(6);
        for (int i = 0; i < 6; i++) {
            sb.append(chars.charAt(rnd.nextInt(chars.length())));
        }
        return sb.toString();
    }

    private void handleCreateQuestion(User user, BufferedReader in, PrintWriter out) throws IOException, SQLException {
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

            out.println("CREATE_QUESTION_OK Access code: " + accessCode);


        } catch (SQLException e) {
            conn.rollback();
            e.printStackTrace();
            out.println(ClientServerProtocol.buildError("DB_ERROR_CREATING_QUESTION"));
        } finally {
            conn.setAutoCommit(oldAutoCommit);
        }
    }

    private void handleAnswerQuestion(User user, BufferedReader in, PrintWriter out) throws IOException {
        if (!"STUDENT".equalsIgnoreCase(user.role())) {
            out.println(ClientServerProtocol.buildError("ONLY_STUDENTS_CAN_ANSWER"));
            return;
        }

        out.println("Access code:");
        String accessCode = in.readLine();
        if (accessCode == null || accessCode.isBlank()) {
            out.println(ClientServerProtocol.buildError("MISSING_ACCESS_CODE"));
            return;
        }

        try {
            Connection conn = dbManager.getConnection();

            // 1) Find question by access code
            long questionId;
            String statement;
            String startStr;
            String endStr;

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, statement, startTime, endTime FROM questions WHERE accessCode = ?")) {
                ps.setString(1, accessCode.trim());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        out.println(ClientServerProtocol.buildError("QUESTION_NOT_FOUND"));
                        return;
                    }
                    questionId = rs.getLong("id");
                    statement = rs.getString("statement");
                    startStr = rs.getString("startTime");
                    endStr = rs.getString("endTime");
                }
            }

            // 2) Check if question is active (using ISO date-time strings)
            java.time.LocalDateTime now = java.time.LocalDateTime.now();
            java.time.LocalDateTime start = java.time.LocalDateTime.parse(startStr);
            java.time.LocalDateTime end = java.time.LocalDateTime.parse(endStr);
            if (now.isBefore(start) || now.isAfter(end)) {
                out.println(ClientServerProtocol.buildError("QUESTION_NOT_ACTIVE"));
                return;
            }

            // 3) Check if this student already answered this question
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

            // 4) Show question and options
            out.println("Question: " + statement);
            out.println("Options:");
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT code, text FROM options WHERE questionId = ? ORDER BY code")) {
                ps.setLong(1, questionId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String code = rs.getString("code");
                        String text = rs.getString("text");
                        out.println(code + ") " + text);
                    }
                }
            }

            // 5) Ask for option code
            out.println("Enter option code:");
            String optionCode = in.readLine();
            if (optionCode == null || optionCode.isBlank()) {
                out.println(ClientServerProtocol.buildError("MISSING_OPTION_CODE"));
                return;
            }
            optionCode = optionCode.trim();

            // 6) Validate option code
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM options WHERE questionId = ? AND code = ?")) {
                ps.setLong(1, questionId);
                ps.setString(2, optionCode);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next() || rs.getInt(1) == 0) {
                        out.println(ClientServerProtocol.buildError("INVALID_OPTION_CODE"));
                        return;
                    }
                }
            }

            // 7) Insert answer
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO answers(studentId, questionId, optionCode, timestamp) VALUES (?,?,?,?)")) {
                ps.setLong(1, user.id());
                ps.setLong(2, questionId);
                ps.setString(3, optionCode);
                ps.setString(4, java.time.LocalDateTime.now().toString());
                ps.executeUpdate();
            }

            out.println("ANSWER_OK");

            // TODO:
            // - atualizar dbVersion
            // - enviar heartbeat com a query SQL para réplicas

        } catch (SQLException e) {
            e.printStackTrace();
            out.println(ClientServerProtocol.buildError("DB_ERROR_ANSWERING"));
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
            try (
                    Socket s = this.socket;
                    BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    PrintWriter out = new PrintWriter(s.getOutputStream(), true)
            ) {

                User user = null;

                while (true) {
                    String line = in.readLine();
                    if (line == null)
                        break;

                    String[] parts = line.trim().split("\\s+");
                    String cmd = parts[0].toUpperCase();

                    switch (cmd) {
                        case "LOGIN" -> {
                            user = handleLogin(parts, in, out);
                            // If login succeeded, user != null
                        }

                        case "REGISTER_STUDENT" -> handleRegisterStudent(parts, out);
                        case "REGISTER_TEACHER" -> handleRegisterTeacher(parts, out);

                        default -> {
                            if (user == null) {
                                out.println(ClientServerProtocol.buildError("NOT_AUTHENTICATED"));
                            } else {
                                out.println("ECHO " + line);
                            }
                        }
                    }
                }

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
