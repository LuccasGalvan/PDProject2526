package pt.isec.pdG36.proj.server.services;

import pt.isec.pdG36.proj.common.protocol.ClientServerProtocol;
import pt.isec.pdG36.proj.server.Server;
import pt.isec.pdG36.proj.server.db.*;
import pt.isec.pdG36.proj.server.util.QuestionExportData;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static pt.isec.pdG36.proj.server.util.ServerUtil.escapeSqlLiteral;
import static pt.isec.pdG36.proj.server.util.ServerUtil.toCsvField;

public class TeacherService {
    private final DatabaseManager dbManager;
    private final Object dbLock;
    private final Server server;

    public TeacherService(DatabaseManager dbManager, Object dbLock, Server server) {
        this.dbManager = dbManager;
        this.dbLock = dbLock;
        this.server = server;
    }

    public void handleCreateQuestion(User user, BufferedReader in, PrintWriter out) throws IOException, SQLException {
        if (!server.isPrimary()) {
            out.println(ClientServerProtocol.buildError("Primary server is down, please retry in a few seconds"));
            return;
        }

        // Only teachers can create questions
        if (!"TEACHER".equalsIgnoreCase(user.role())) {
            out.println(ClientServerProtocol.buildError("NOT_A_TEACHER"));
            return;
        }

        out.println("Enter question statement:");
        String statement = in.readLine();
        if (statement == null || statement.isBlank()) {
            out.println(ClientServerProtocol.buildError("EMPTY_STATEMENT"));
            return;
        }

        out.println("Enter start time (e.g., 2025-01-01T10:00):");
        String startTime = in.readLine();
        out.println("Enter end time (e.g., 2025-01-01T10:10):");
        String endTime = in.readLine();

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

        String[] codes = new String[numOptions];
        String[] texts = new String[numOptions];
        boolean[] correctFlags = new boolean[numOptions];
        boolean foundCorrect = false;

        for (int i = 0; i < numOptions; i++) {
            out.println("Option " + (i + 1) + " - code (e.g., A, B, C):");
            codes[i] = in.readLine();

            out.println("Option " + (i + 1) + " - text:");
            texts[i] = in.readLine();

            if (!foundCorrect) {
                // Ask until we get a valid yes/no answer
                while (true) {
                    out.println("Is this the correct option? (yes/no):");
                    String ans = in.readLine();
                    if (ans == null) {
                        out.println(ClientServerProtocol.buildError("ABORTED"));
                        return;
                    }
                    ans = ans.trim();
                    if (ans.equalsIgnoreCase("yes")) {
                        correctFlags[i] = true;
                        foundCorrect = true;
                        break;
                    } else if (ans.equalsIgnoreCase("no")) {
                        correctFlags[i] = false;
                        break;
                    } else {
                        out.println("Please answer 'yes' or 'no'.");
                    }
                }
            } else {
                correctFlags[i] = false;
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

        String accessCode = generateAccessCode();

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
                    if (server.isPrimary()) {
                        {
                            long newVersion = server.bumpDbVersion();

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

                            server.sendSqlHeartbeatToSecondaries(sqlQuestion);
                        }

                        for (int i = 0; i < numOptions; i++) {
                            long newVersion = server.bumpDbVersion();

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

                            server.sendSqlHeartbeatToSecondaries(sqlOption);
                        }
                    }

                    out.println("CREATE_QUESTION_OK Access code: " + accessCode);
                    server.notifyStudents("NEW_QUESTION " + accessCode);
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

    public void handleListMyQuestions(User user, String[] parts, PrintWriter out) throws SQLException {
        String filter = "ALL"; // could be changed to filter but i really don't have the time
        if (parts.length >= 2) {
            filter = parts[1].toUpperCase(); // SCHEDULED | ONGOING | CLOSED | ALL
        }

        java.time.LocalDateTime now = java.time.LocalDateTime.now();

        synchronized (dbLock) {
            java.util.List<QuestionDTO> questions =
                    dbManager.findQuestionsByTeacher(user.id());

            StringBuilder sb = new StringBuilder();
            if (questions.isEmpty()) {
                sb.append("== You have not created any questions yet ==\n");
            } else {
                sb.append("== Your questions ==\n");
                int idx = 1;
                for (QuestionDTO q : questions) {
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

    public void handleViewResults(User user, BufferedReader in, PrintWriter out) throws IOException {
        if (!"TEACHER".equalsIgnoreCase(user.role())) {
            out.println(ClientServerProtocol.buildError("NOT_A_TEACHER"));
            return;
        }

        out.println("PROMPT Access code of question:");
        String accessCode = in.readLine();
        if (accessCode == null || accessCode.isBlank()) {
            out.println(ClientServerProtocol.buildError("MISSING_ACCESS_CODE"));
            return;
        }
        accessCode = accessCode.trim();

        synchronized (dbLock) {
            try {
                QuestionDTO q = dbManager.findQuestionByAccessCode(accessCode);
                if (q == null) {
                    out.println(ClientServerProtocol.buildError("QUESTION_NOT_FOUND"));
                    return;
                }

                if (q.teacherId() != user.id()) {
                    out.println(ClientServerProtocol.buildError("NOT_YOUR_QUESTION"));
                    return;
                }

                java.time.LocalDateTime now = java.time.LocalDateTime.now();
                java.time.LocalDateTime end = java.time.LocalDateTime.parse(q.endTime());
                if (now.isBefore(end)) {
                    out.println(ClientServerProtocol.buildError("QUESTION_NOT_EXPIRED"));
                    return;
                }

                java.util.List<OptionDTO> opts =
                        dbManager.findOptionsForQuestion(q.id());

                java.util.List<AnswerResultRow> answers =
                        dbManager.findAnswersForQuestion(q.id());

                StringBuilder sb = new StringBuilder();

                sb.append("Results for question ").append(q.accessCode()).append("\n");
                sb.append("Statement: ").append(q.statement()).append("\n");
                sb.append("Start: ").append(q.startTime()).append("\n");
                sb.append("End: ").append(q.endTime()).append("\n");
                sb.append("\nOptions:\n");

                for (OptionDTO o : opts) {
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
                    for (AnswerResultRow row : answers) {
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

    public void handleEditQuestion(User user, BufferedReader in, PrintWriter out) throws IOException, SQLException {
        if (!server.isPrimary()) {
            out.println(ClientServerProtocol.buildError("Primary server is down, please retry in a few seconds"));
            return;
        }

        if (!"TEACHER".equalsIgnoreCase(user.role())) {
            out.println(ClientServerProtocol.buildError("NOT_A_TEACHER"));
            return;
        }

        out.println("PROMPT Access code of question to edit:");
        String accessCode = in.readLine();
        if (accessCode == null || accessCode.isBlank()) {
            out.println(ClientServerProtocol.buildError("MISSING_ACCESS_CODE"));
            return;
        }
        accessCode = accessCode.trim();

        QuestionDTO q;
        int answersCount;

        synchronized (dbLock) {
            q = dbManager.findQuestionByAccessCode(accessCode);
            if (q == null) {
                out.println(ClientServerProtocol.buildError("QUESTION_NOT_FOUND"));
                return;
            }
            if (q.teacherId() != user.id()) {
                out.println(ClientServerProtocol.buildError("NOT_OWNER_OF_QUESTION"));
                return;
            }
            answersCount = dbManager.countAnswersForQuestion(q.id());
        }

        if (answersCount > 0) {
            out.println(ClientServerProtocol.buildError("QUESTION_HAS_ANSWERS_CANNOT_EDIT"));
            return;
        }

        out.println("Enter NEW question statement:");
        String statement = in.readLine();
        if (statement == null || statement.isBlank()) {
            out.println(ClientServerProtocol.buildError("EMPTY_STATEMENT"));
            return;
        }

        out.println("Enter NEW start time (e.g., 2025-01-01T10:00):");
        String startTime = in.readLine();
        out.println("Enter NEW end time (e.g., 2025-01-01T10:10):");
        String endTime = in.readLine();

        int numOptions = 0;
        while (true) {
            out.println("Enter number of options (2..5):");
            String s = in.readLine();
            if (s == null) {
                out.println(ClientServerProtocol.buildError("CANCELLED"));
                return;
            }
            try {
                numOptions = Integer.parseInt(s.trim());
                if (numOptions < 2 || numOptions > 5) {
                    out.println(ClientServerProtocol.buildError("INVALID_NUM_OPTIONS"));
                } else {
                    break;
                }
            } catch (NumberFormatException e) {
                out.println(ClientServerProtocol.buildError("INVALID_NUM_OPTIONS"));
            }
        }

        String[] codes = new String[numOptions];
        String[] texts = new String[numOptions];
        boolean[] correctFlags = new boolean[numOptions];

        boolean hasCorrect = false;

        for (int i = 0; i < numOptions; i++) {
            String code = String.valueOf((char)('A' + i));
            out.println("Option " + code + " text:");
            String txt = in.readLine();
            if (txt == null || txt.isBlank()) {
                out.println(ClientServerProtocol.buildError("EMPTY_OPTION_TEXT"));
                return;
            }

            out.println("Is this a correct option? (yes/no):");
            String ans = in.readLine();
            boolean isCorrect = ans != null && ans.trim().equalsIgnoreCase("yes");

            codes[i] = code;
            texts[i] = txt;
            correctFlags[i] = isCorrect;
            if (isCorrect) hasCorrect = true;
        }

        if (!hasCorrect) {
            out.println(ClientServerProtocol.buildError("NO_CORRECT_OPTION"));
            return;
        }

        synchronized (dbLock) {
            Connection conn = dbManager.getConnection();
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                // Update question core fields (statement + times)
                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE questions SET statement = ?, startTime = ?, endTime = ? WHERE id = ?")) {
                    ps.setString(1, statement);
                    ps.setString(2, startTime);
                    ps.setString(3, endTime);
                    ps.setLong(4, q.id());
                    ps.executeUpdate();
                }

                // Delete old options
                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM options WHERE questionId = ?")) {
                    ps.setLong(1, q.id());
                    ps.executeUpdate();
                }

                // Insert new options
                for (int i = 0; i < numOptions; i++) {
                    dbManager.insertOption(q.id(), codes[i], texts[i], correctFlags[i]);
                }

                conn.commit();

                // Replication: UPDATE question, then delete+reinsert options
                if (server.isPrimary()) {
                    {
                        long newVersion = server.bumpDbVersion();
                        String sql = "UPDATE questions SET "
                                + "statement='" + escapeSqlLiteral(statement) + "', "
                                + "startTime='" + escapeSqlLiteral(startTime) + "', "
                                + "endTime='" + escapeSqlLiteral(endTime) + "' "
                                + "WHERE id=" + q.id();
                        server.sendSqlHeartbeatToSecondaries(sql);
                    }

                    {
                        long newVersion = server.bumpDbVersion();
                        String sql = "DELETE FROM options WHERE questionId=" + q.id();
                        server.sendSqlHeartbeatToSecondaries(sql);
                    }

                    for (int i = 0; i < numOptions; i++) {
                        long newVersion = server.bumpDbVersion();
                        String sql = "INSERT INTO options(questionId, code, text, isCorrect) VALUES ("
                                + q.id() + ","
                                + "'" + escapeSqlLiteral(codes[i]) + "',"
                                + "'" + escapeSqlLiteral(texts[i]) + "',"
                                + (correctFlags[i] ? 1 : 0)
                                + ")";
                        server.sendSqlHeartbeatToSecondaries(sql);
                    }
                }

                out.println("BLOCK Question edited successfully. Access code: " + accessCode);

            } catch (SQLException e) {
                conn.rollback();
                e.printStackTrace();
                out.println(ClientServerProtocol.buildError("EDIT_QUESTION_DB_ERROR"));
            } finally {
                conn.setAutoCommit(oldAuto);
            }
        }
    }

    public void handleDeleteQuestion(User user, BufferedReader in, PrintWriter out) throws IOException, SQLException {
        if (!server.isPrimary()) {
            out.println(ClientServerProtocol.buildError("Primary server is down, please retry in a few seconds"));
            return;
        }

        if (!"TEACHER".equalsIgnoreCase(user.role())) {
            out.println(ClientServerProtocol.buildError("NOT_A_TEACHER"));
            return;
        }

        out.println("PROMPT Access code of question to delete:");
        String accessCode = in.readLine();
        if (accessCode == null || accessCode.isBlank()) {
            out.println(ClientServerProtocol.buildError("MISSING_ACCESS_CODE"));
            return;
        }
        accessCode = accessCode.trim();

        QuestionDTO q;
        int answersCount;

        synchronized (dbLock) {
            q = dbManager.findQuestionByAccessCode(accessCode);
            if (q == null) {
                out.println(ClientServerProtocol.buildError("QUESTION_NOT_FOUND"));
                return;
            }
            if (q.teacherId() != user.id()) {
                out.println(ClientServerProtocol.buildError("NOT_OWNER_OF_QUESTION"));
                return;
            }
            answersCount = dbManager.countAnswersForQuestion(q.id());
        }

        if (answersCount > 0) {
            out.println(ClientServerProtocol.buildError("QUESTION_HAS_ANSWERS_CANNOT_DELETE"));
            return;
        }

        synchronized (dbLock) {
            Connection conn = dbManager.getConnection();
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                // Since we checked there are no answers, this is mostly defensive
                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM answers WHERE questionId = ?")) {
                    ps.setLong(1, q.id());
                    ps.executeUpdate();
                }

                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM options WHERE questionId = ?")) {
                    ps.setLong(1, q.id());
                    ps.executeUpdate();
                }

                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM questions WHERE id = ?")) {
                    ps.setLong(1, q.id());
                    ps.executeUpdate();
                }

                conn.commit();

                if (server.isPrimary()) {
                    {
                        long newVersion = server.bumpDbVersion();
                        String sql = "DELETE FROM answers WHERE questionId=" + q.id();
                        server.sendSqlHeartbeatToSecondaries(sql);
                    }
                    {
                        long newVersion = server.bumpDbVersion();
                        String sql = "DELETE FROM options WHERE questionId=" + q.id();
                        server.sendSqlHeartbeatToSecondaries(sql);
                    }
                    {
                        long newVersion = server.bumpDbVersion();
                        String sql = "DELETE FROM questions WHERE id=" + q.id();
                        server.sendSqlHeartbeatToSecondaries(sql);
                    }
                }

                out.println("BLOCK Question deleted successfully. Access code: " + accessCode);

            } catch (SQLException e) {
                conn.rollback();
                e.printStackTrace();
                out.println(ClientServerProtocol.buildError("DELETE_QUESTION_DB_ERROR"));
            } finally {
                conn.setAutoCommit(oldAuto);
            }
        }
    }

    public void handleExportResults(User user, BufferedReader in, PrintWriter out) throws IOException {
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

        QuestionDTO q;
        java.util.List<OptionDTO> opts;
        java.util.List<AnswerResultRow> answers;

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

    public void handleExportAllResults(User user, PrintWriter out) {
        if (!"TEACHER".equalsIgnoreCase(user.role())) {
            out.println(ClientServerProtocol.buildError("NOT_A_TEACHER"));
            return;
        }

        java.time.LocalDateTime now = java.time.LocalDateTime.now();
        java.util.List<QuestionExportData> toExport = new java.util.ArrayList<>();

        // load all closed questions + their data under lock
        synchronized (dbLock) {
            try {
                java.util.List<QuestionDTO> questions =
                        dbManager.findQuestionsByTeacher(user.id());

                for (QuestionDTO q : questions) {
                    java.time.LocalDateTime end = java.time.LocalDateTime.parse(q.endTime());
                    if (!now.isAfter(end)) {
                        continue;
                    }

                    java.util.List<OptionDTO> opts =
                            dbManager.findOptionsForQuestion(q.id());
                    java.util.List<AnswerResultRow> answers =
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

    private String generateAccessCode() {
        String chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
        java.util.Random rnd = new java.util.Random();
        StringBuilder sb = new StringBuilder(6);
        for (int i = 0; i < 6; i++) {
            sb.append(chars.charAt(rnd.nextInt(chars.length())));
        }
        return sb.toString();
    }

    // Writes ONE question (metadata + options + answers) in CSV format to pw
    private void writeQuestionResultsToCsv(
            PrintWriter pw,
            QuestionDTO q,
            java.util.List<OptionDTO> opts,
            java.util.List<AnswerResultRow> answers
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
        for (OptionDTO o : opts) {
            pw.println(
                    toCsvField(o.code()) + "," +
                            toCsvField(o.text()) + "," +
                            toCsvField(o.isCorrect() ? "true" : "false")
            );
        }

        pw.println();
        pw.println("Timestamp,StudentNumber,StudentName,StudentEmail,OptionCode,Correct");
        for (AnswerResultRow row : answers) {
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
}
