package pt.isec.pdG36.proj.server.services;

import pt.isec.pdG36.proj.common.protocol.ClientServerProtocol;
import pt.isec.pdG36.proj.server.Server;
import pt.isec.pdG36.proj.server.db.DatabaseManager;
import pt.isec.pdG36.proj.server.db.StudentAnswerDTO;
import pt.isec.pdG36.proj.server.db.User;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static pt.isec.pdG36.proj.server.util.ServerUtil.escapeSqlLiteral;

public class StudentService {
    private final DatabaseManager dbManager;
    private final Object dbLock;
    private final Server server;

    public StudentService(DatabaseManager dbManager, Object dbLock, Server server) {
        this.dbManager = dbManager;
        this.dbLock = dbLock;
        this.server = server;
    }

    public void handleListMyAnswers(User user, PrintWriter out) throws SQLException {
        synchronized (dbLock) {
            java.util.List<StudentAnswerDTO> answers =
                    dbManager.findClosedAnswersForStudent(user.id());

            StringBuilder sb = new StringBuilder();
            if (answers.isEmpty()) {
                sb.append("== You have no answered questions whose answering time has expired ==\n");
            } else {
                sb.append("== Your answered questions (closed) ==\n");
                int idx = 1;
                for (StudentAnswerDTO a : answers) {
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

    public void handleAnswerQuestion(User user, BufferedReader in, PrintWriter out) throws IOException {
        if (!server.isPrimary()) {
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

        StringBuilder sb = new StringBuilder();
        sb.append("Question: ").append(statement).append("\n");
        sb.append("Options:").append("\n");
        for (String[] opt : options) {
            sb.append(opt[0]).append(") ").append(opt[1]).append("\n");
        }
        String questionText = sb.toString().replace("\r", "").replace("\n", "\\n");
        out.println("QUESTION_BLOCK " + questionText);

        String optionCode = in.readLine();
        if (optionCode == null || optionCode.isBlank()) {
            out.println(ClientServerProtocol.buildError("MISSING_OPTION_CODE"));
            out.println("QUESTION_ABORTED");
            return;
        }
        optionCode = optionCode.trim();

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
                    long newVersion = server.bumpDbVersion();
                    if (server.isPrimary()) {
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
                        server.sendSqlHeartbeatToSecondaries(sqlForReplication);
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
}
