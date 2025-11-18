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
        if (conn != null && !conn.isClosed())
            return;

        String url = "jdbc:sqlite:" + dbPath.toAbsolutePath();
        conn = DriverManager.getConnection(url);
        conn.setAutoCommit(true); // ok for now
    }

    public void initSchema() throws SQLException {
        try (Statement st = conn.createStatement()) {
            // Example user table (teacher + student in one)
            st.executeUpdate("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    role TEXT NOT NULL,             -- 'TEACHER' or 'STUDENT'
                    student_number INTEGER,         -- only for students
                    name TEXT NOT NULL,
                    email TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL
                )
            """);

            // Optional config table with teacher registration code hash
            st.executeUpdate("""
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """);
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