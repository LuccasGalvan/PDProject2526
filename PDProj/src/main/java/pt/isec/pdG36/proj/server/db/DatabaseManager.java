package pt.isec.pdG36.proj.server.db;

import java.nio.file.Path;
import java.sql.*;

public class DatabaseManager implements AutoCloseable {
    private final Path dbPath;
    private Connection conn;

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

            stmt.executeUpdate("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL CHECK (type IN ('teacher', 'student')),
                number INTEGER,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                passwordHash TEXT NOT NULL
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

            System.out.println("[DB] Schema initialized.");
        }
    }

    public boolean registerStudent(int number, String name, String email, String password) throws SQLException {
        String sql = "INSERT INTO users(role, student_number, name, email, password_hash) VALUES (?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "STUDENT");
            ps.setInt(2, number);
            ps.setString(3, name);
            ps.setString(4, email);
            ps.setString(5, PassUtil.hashPassword(password));
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            // duplicate email or number etc.
            return false;
        }
    }

    public boolean registerTeacher(String name, String email, String password, String teacherCodeHash) throws SQLException {
        // OPTIONAL for now: validate teacherCodeHash vs config table
        String sql = "INSERT INTO users(role, student_number, name, email, password_hash) VALUES (?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "TEACHER");
            ps.setNull(2, java.sql.Types.INTEGER);
            ps.setString(3, name);
            ps.setString(4, email);
            ps.setString(5, PassUtil.hashPassword(password));
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
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