package pt.isec.pdG36.proj.server.services;

import pt.isec.pdG36.proj.common.protocol.ClientServerProtocol;
import pt.isec.pdG36.proj.server.Server;
import pt.isec.pdG36.proj.server.db.DatabaseManager;
import pt.isec.pdG36.proj.server.db.PassUtil;
import pt.isec.pdG36.proj.server.db.RegisterResult;
import pt.isec.pdG36.proj.server.db.User;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static pt.isec.pdG36.proj.server.util.ServerUtil.escapeSqlLiteral;

public class UserService {
    private final DatabaseManager dbManager;
    private final Object dbLock;
    private final Server server; // only if you need bumpDbVersion / replication

    public UserService(DatabaseManager dbManager, Object dbLock, Server server) {
        this.dbManager = dbManager;
        this.dbLock = dbLock;
        this.server = server;
    }

    public User handleLogin(String[] parts, BufferedReader in, PrintWriter out) {
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

    public void handleRegisterStudent(String[] parts, PrintWriter out) {
        if (!server.isPrimary()) {
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
                RegisterResult res = dbManager.registerStudent(number, name, email, password);
                switch (res) {
                    case OK -> {
                        try {
                            long newVersion = server.bumpDbVersion();

                            if (server.isPrimary()) {
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

                                server.sendSqlHeartbeatToSecondaries(sqlForReplication);
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

    public void handleRegisterTeacher(String[] parts, PrintWriter out) {
        if (!server.isPrimary()) {
            out.println(ClientServerProtocol.buildError("Primary server is down, please retry in a few seconds"));
            return;
        }

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
                RegisterResult res = dbManager.registerTeacher(name, email, password, teacherCode);
                switch (res) {
                    case OK -> {
                        try {
                            long newVersion = server.bumpDbVersion();

                            if (server.isPrimary()) {
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

                                server.sendSqlHeartbeatToSecondaries(sqlForReplication);
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
                    case RegisterResult.INVALID_TEACHER_CODE ->
                            out.println(ClientServerProtocol.buildRegisterFail("INVALID_TEACHER_CODE"));
                    default -> out.println(ClientServerProtocol.buildRegisterFail("DB_ERROR"));
                }
            } catch (Exception e) {
                out.println(ClientServerProtocol.buildError("DB_ERROR"));
            }
        }
    }

    public void handleEditProfile(User user, BufferedReader in, PrintWriter out) throws IOException {
        if (!server.isPrimary()) {
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

                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE users SET name = ?, email = ?, password_hash = ? WHERE id = ?")) {
                    ps.setString(1, finalName);
                    ps.setString(2, finalEmail);
                    ps.setString(3, finalHash);
                    ps.setLong(4, user.id());
                    ps.executeUpdate();
                }

                try {
                    long newVersion = server.bumpDbVersion();
                    if (server.isPrimary()) {
                        String sql = "UPDATE users SET "
                                + "name='" + escapeSqlLiteral(finalName) + "', "
                                + "email='" + escapeSqlLiteral(finalEmail) + "', "
                                + "password_hash='" + escapeSqlLiteral(finalHash) + "' "
                                + "WHERE id=" + user.id();
                        server.sendSqlHeartbeatToSecondaries(sql);
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
}
