package pt.isec.pdG36.proj.server.db;

import java.nio.file.Path;
import java.sql.*;
import java.util.List;
import java.util.UUID;

public class DatabaseManager implements AutoCloseable {
    private final Path dbPath;
    private Connection conn;
    private static final String DEFAULT_TEACHER_CODE = "PD2025";

    public DatabaseManager(Path dbPath) {
        this.dbPath = dbPath;
    }

    public void connect() throws SQLException {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new SQLException("SQLite JDBC driver not found on classpath", e);
        }

        String url = "jdbc:sqlite:" + dbPath.toAbsolutePath();
        this.conn = DriverManager.getConnection(url);
    }

    public Path getDbFile() {
        return dbPath;
    }

    // Resultado explícito para operações de registo
    public enum RegisterResult {
        OK,
        EMAIL_IN_USE,
        NUMBER_IN_USE,
        INVALID_TEACHER_CODE,
        DB_ERROR
    }

    // This will only create the tables for the database
    public void initSchema() throws SQLException {
        try (Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("""
            CREATE TABLE IF NOT EXISTS version (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL
            );
        """);

            stmt.executeUpdate("""
            INSERT OR IGNORE INTO version (id, version)
            VALUES (1, 0);
        """);

            // config: key/value para configurações simples
            stmt.executeUpdate("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
        """);

            // users: garantir UNIQUE em email e student_number
            stmt.executeUpdate("""
            CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            role TEXT NOT NULL CHECK (role IN ('TEACHER','STUDENT')),
            student_number INTEGER UNIQUE,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL
            );
        """);

            stmt.executeUpdate("""
            CREATE TABLE IF NOT EXISTS questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                teacherId INTEGER NOT NULL,
                statement TEXT NOT NULL,
                startTime TEXT NOT NULL,
                endTime TEXT NOT NULL,
                accessCode TEXT NOT NULL UNIQUE,
                FOREIGN KEY (teacherId) REFERENCES users(id)
            );
        """);

            stmt.executeUpdate("""
            CREATE TABLE IF NOT EXISTS options (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                questionId INTEGER NOT NULL,
                code TEXT NOT NULL,
                text TEXT NOT NULL,
                isCorrect INTEGER NOT NULL CHECK (isCorrect IN (0,1)),
                FOREIGN KEY (questionId) REFERENCES questions(id)
            );
        """);

            stmt.executeUpdate("""
            CREATE TABLE IF NOT EXISTS answers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                studentId INTEGER NOT NULL,
                questionId INTEGER NOT NULL,
                optionCode TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (studentId) REFERENCES users(id),
                FOREIGN KEY (questionId) REFERENCES questions(id)
            );
        """);

            // Garantir valor por defeito para TEACHER_CODE_HASH
            String defaultCodeHash = PassUtil.hashPassword(DEFAULT_TEACHER_CODE);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)")) {
                ps.setString(1, "TEACHER_CODE_HASH");
                ps.setString(2, defaultCodeHash);
                ps.executeUpdate();
            }

            System.out.println("[DB] Schema initialized.");
        }
    }

    public RegisterResult registerStudent(int number, String name, String email, String password) throws SQLException {
        String sql = "INSERT INTO users(role, student_number, name, email, password_hash) VALUES (?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "STUDENT");
            ps.setInt(2, number);
            ps.setString(3, name);
            ps.setString(4, email);
            ps.setString(5, PassUtil.hashPassword(password));
            ps.executeUpdate();

            return RegisterResult.OK;
        } catch (SQLException e) {
            String msg = e.getMessage();
            if (msg != null) {
                if (msg.contains("student_number") || msg.contains("users.student_number")) {
                    return RegisterResult.NUMBER_IN_USE;
                }
                if (msg.contains("email") || msg.contains("users.email")) {
                    return RegisterResult.EMAIL_IN_USE;
                }
            }
            return RegisterResult.DB_ERROR;
        }
    }

    public RegisterResult registerTeacher(String name, String email, String password, String teacherCode) throws SQLException {
        // 1) obter TEACHER_CODE_HASH da tabela config
        String storedHash = null;
        try (PreparedStatement ps = conn.prepareStatement("SELECT value FROM config WHERE key = ?")) {
            ps.setString(1, "TEACHER_CODE_HASH");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    storedHash = rs.getString(1);
                }
            }
        }

        if (storedHash == null) {
            // configuração ausente -> erro DB
            return RegisterResult.DB_ERROR;
        }

        // 2) comparar hashes
        String providedHash = PassUtil.hashPassword(teacherCode);
        if (!storedHash.equals(providedHash)) {
            return RegisterResult.INVALID_TEACHER_CODE;
        }

        // 3) se ok, inserir professor
        String sql = "INSERT INTO users(role, student_number, name, email, password_hash) VALUES (?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "TEACHER");
            ps.setNull(2, java.sql.Types.INTEGER);
            ps.setString(3, name);
            ps.setString(4, email);
            ps.setString(5, PassUtil.hashPassword(password));
            ps.executeUpdate();

            return RegisterResult.OK;
        } catch (SQLException e) {
            String msg = e.getMessage();
            if (msg != null) {
                if (msg.contains("student_number") || msg.contains("users.student_number")) {
                    return RegisterResult.NUMBER_IN_USE;
                }
                if (msg.contains("email") || msg.contains("users.email")) {
                    return RegisterResult.EMAIL_IN_USE;
                }
            }
            return RegisterResult.DB_ERROR;
        }
    }

    public String getDbPath() {
        return dbPath.toString();
    }

    public User authenticate(String email, String password) throws SQLException {
        String sql = "SELECT id, role, student_number, name, email, password_hash FROM users WHERE email = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, email);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next())
                    return null;

                String storedHash = rs.getString("password_hash");
                String providedHash = PassUtil.hashPassword(password);
                if (!storedHash.equals(providedHash))
                    return null;

                long id = rs.getLong("id");
                String role = rs.getString("role");
                int studentNumber = rs.getInt("student_number");
                Integer sn = rs.wasNull() ? null : studentNumber;
                String name = rs.getString("name");
                String em = rs.getString("email");

                return new User(id, role, sn, name, em);
            }
        }
    }

    public long insertQuestion(long teacherId,
                               String statement,
                               String startTime,
                               String endTime,
                               String accessCode) throws SQLException {
        String sql = """
                INSERT INTO questions (teacherId, statement, startTime, endTime, accessCode)
                VALUES (?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setLong(1, teacherId);
            ps.setString(2, statement);
            ps.setString(3, startTime);
            ps.setString(4, endTime);
            ps.setString(5, accessCode);
            ps.executeUpdate();


            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                } else {
                    throw new SQLException("Failed to obtain generated question id");
                }
            }
        }
    }

    public void insertOption(long questionId,
                             String code,
                             String text,
                             boolean isCorrect) throws SQLException {
        String sql = """
                INSERT INTO options (questionId, code, text, isCorrect)
                VALUES (?, ?, ?, ?)
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, questionId);
            ps.setString(2, code);
            ps.setString(3, text);
            ps.setInt(4, isCorrect ? 1 : 0);
            ps.executeUpdate();

        }
    }

    public record StudentAnswerDTO(
            long questionId,
            String statement,
            String startTime,
            String endTime,
            String optionCode,
            boolean correct
    ) {}

    public record QuestionDTO(
            long id,
            long teacherId,
            String statement,
            String startTime,
            String endTime,
            String accessCode
    ) {}

    public record OptionDTO(
            long id,
            long questionId,
            String code,
            String text,
            boolean isCorrect
    ) {}

    public record AnswerResultRow(
            String timestamp,
            Integer studentNumber,
            String studentName,
            String studentEmail,
            String optionCode,
            boolean correct
    ) {}

    public QuestionDTO findQuestionByAccessCode(String accessCode) throws SQLException {
        String sql = """
                SELECT id, teacherId, statement, startTime, endTime, accessCode
                FROM questions
                WHERE accessCode = ?
                """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, accessCode);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next())
                    return null;

                long id = rs.getLong("id");
                long teacherId = rs.getLong("teacherId");
                String statement = rs.getString("statement");
                String startTime = rs.getString("startTime");
                String endTime = rs.getString("endTime");
                String code = rs.getString("accessCode");

                return new QuestionDTO(id, teacherId, statement, startTime, endTime, code);
            }
        }
    }

    public java.util.List<StudentAnswerDTO> findClosedAnswersForStudent(long studentId) throws SQLException {
        String sql = """
                SELECT
                    q.id            AS qid,
                    q.statement     AS statement,
                    q.startTime     AS startTime,
                    q.endTime       AS endTime,
                    a.optionCode    AS optionCode,
                    o.isCorrect     AS isCorrect
                FROM answers a
                JOIN questions q
                     ON q.id = a.questionId
                LEFT JOIN options o
                     ON o.questionId = q.id
                    AND o.code = a.optionCode
                WHERE a.studentId = ?
                  AND q.endTime < ?
                ORDER BY q.endTime DESC
                """;

        String now = java.time.LocalDateTime.now().toString();

        java.util.List<StudentAnswerDTO> list = new java.util.ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, studentId);
            ps.setString(2, now);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long qid = rs.getLong("qid");
                    String stmt = rs.getString("statement");
                    String start = rs.getString("startTime");
                    String end = rs.getString("endTime");
                    String optionCode = rs.getString("optionCode");

                    boolean correct = false;
                    int isCorrectInt = rs.getInt("isCorrect");
                    if (!rs.wasNull()) {
                        correct = (isCorrectInt == 1);
                    }

                    list.add(new StudentAnswerDTO(
                            qid, stmt, start, end, optionCode, correct
                    ));
                }
            }
        }

        return list;
    }

    public java.util.List<QuestionDTO> findQuestionsByTeacher(long teacherId) throws SQLException {
        String sql = """
                SELECT id, teacherId, statement, startTime, endTime, accessCode
                FROM questions
                WHERE teacherId = ?
                ORDER BY startTime DESC
                """;

        java.util.List<QuestionDTO> list = new java.util.ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, teacherId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long id = rs.getLong("id");
                    long tid = rs.getLong("teacherId");
                    String statement = rs.getString("statement");
                    String start = rs.getString("startTime");
                    String end = rs.getString("endTime");
                    String code = rs.getString("accessCode");

                    list.add(new QuestionDTO(id, tid, statement, start, end, code));
                }
            }
        }

        return list;
    }

    public java.util.List<AnswerResultRow> findAnswersForQuestion(long questionId) throws SQLException {
        String sql = """
            SELECT a.timestamp,
                   u.student_number,
                   u.name,
                   u.email,
                   a.optionCode,
                   CASE WHEN o.isCorrect = 1 THEN 1 ELSE 0 END AS isCorrectAnswer
            FROM answers a
            JOIN users u ON u.id = a.studentId
            LEFT JOIN options o
                   ON o.questionId = a.questionId
                  AND o.code = a.optionCode
            WHERE a.questionId = ?
            ORDER BY a.timestamp
            """;

        java.util.List<AnswerResultRow> list = new java.util.ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, questionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String ts = rs.getString("timestamp");

                    int studentNum = rs.getInt("student_number");
                    Integer sn = rs.wasNull() ? null : studentNum;

                    String name = rs.getString("name");
                    String email = rs.getString("email");
                    String option = rs.getString("optionCode");
                    boolean correct = rs.getInt("isCorrectAnswer") == 1;

                    list.add(new AnswerResultRow(ts, sn, name, email, option, correct));
                }
            }
        }

        return list;
    }

    public java.util.List<OptionDTO> findOptionsForQuestion(long questionId) throws SQLException {
        String sql = """
                SELECT id, questionId, code, text, isCorrect
                FROM options
                WHERE questionId = ?
                ORDER BY code
                """;

        java.util.List<OptionDTO> list = new java.util.ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, questionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long id = rs.getLong("id");
                    long qid = rs.getLong("questionId");
                    String code = rs.getString("code");
                    String text = rs.getString("text");
                    boolean isCorrect = rs.getInt("isCorrect") == 1;

                    list.add(new OptionDTO(id, qid, code, text, isCorrect));
                }
            }
        }

        return list;
    }

    public void insertAnswer(long studentId, long questionId, String optionCode, String timestamp) throws SQLException {
        String sql = """
                INSERT INTO answers(studentId, questionId, optionCode, timestamp)
                VALUES (?,?,?,?)
                """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, studentId);
            ps.setLong(2, questionId);
            ps.setString(3, optionCode);
            ps.setString(4, timestamp); // standardize later maybe
            ps.executeUpdate();

        }
    }

    // --- DB VERSIONING ----------------------------------------------------

    public long getCurrentDbVersion() throws SQLException {
        String sql = "SELECT version FROM version WHERE id = 1";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                throw new SQLException("version row missing");
            }
            return rs.getLong("version");
        }
    }

    //  increments the db version and returns new value
    public long incrementDbVersion() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE version SET version = version + 1 WHERE id = 1")) {
            int updated = ps.executeUpdate();
            if (updated != 1) {
                throw new SQLException("Failed to update DB version");
            }
        }

        return getCurrentDbVersion();
    }

    @Override
    public void close() throws SQLException {
        if (conn != null && !conn.isClosed())
            conn.close();
    }

    // getters
    public Connection getConnection() {
        return conn;
    }
}

