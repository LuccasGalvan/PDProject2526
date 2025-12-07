package pt.isec.pdG36.proj.client;

import pt.isec.pdG36.proj.common.protocol.ClientServerProtocol;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;

import static java.lang.Thread.sleep;

public class ClientMain {
    static String assignedRole;

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java client.ClientMain <dirIP> <dirUdpPort>");
            return;
        }

        InetAddress dirAddr = InetAddress.getByName(args[0]);
        int dirPort = Integer.parseInt(args[1]);

        BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            ClientConnection conn = new ClientConnection(dirAddr, dirPort);
            if (!conn.connectToPrimary()) {
                System.err.println("[CLIENT] No primary available yet. Retrying in 3 seconds...");
                sleep(3000);
                continue;
            }

            System.out.println();
            System.out.println("=== QUIZ SYSTEM ===");
            System.out.println("1) Login");
            System.out.println("2) Register student");
            System.out.println("3) Register teacher");
            System.out.println("4) Quit");
            System.out.print("Option: ");

            String option = keyboard.readLine();
            if (option == null) {
                conn.close();
                break;
            }
            option = option.trim();

            switch (option) {
                case "1" -> {
                    String role = doLogin(conn, keyboard);
                    if (role != null) {
                        // sessão autenticada nesta ligação
                        postLoginLoop(conn, keyboard, role);
                    }
                    // depois da sessão (ou tentativa), fecha esta ligação
                    conn.close();
                }
                case "2" -> {
                    doRegisterStudent(conn, keyboard);
                    conn.close();
                }
                case "3" -> {
                    doRegisterTeacher(conn, keyboard);
                    conn.close();
                }
                case "4" -> {
                    conn.close();
                    return;
                }
                default -> {
                    System.out.println("Invalid option. Try again.");
                    conn.close();
                }
            }
        }
    }

    private static String doLogin(ClientConnection conn, BufferedReader keyboard) throws Exception {
        System.out.println();
        System.out.println("== Login ==");
        System.out.print("Email: ");
        String email = keyboard.readLine();
        System.out.print("Password: ");
        String password = keyboard.readLine();

        conn.sendLine(ClientServerProtocol.buildLoginRequest(email, password));

        String respLine;
        try {
            respLine = conn.safeReadLine();
            System.out.println("[CLIENT] Raw login response: " + respLine); // DEBUG
        } catch (Exception e) {
            System.err.println("[CLIENT] Error reading login response: " + e.getMessage());
            return null;
        }

        ClientServerProtocol.LoginResponse lr = ClientServerProtocol.parseLoginResponse(respLine);
        if (lr == null) {
            System.err.println("[CLIENT] Invalid response from server: " + respLine);
            return null;
        }

        if (!lr.success()) {
            System.err.println("[CLIENT] Login failed: " + lr.message());
            return null;
        }

        System.out.println("[CLIENT] Login successful. Role: " + lr.role() + " | " + lr.message());
        return (lr.role() != null) ? lr.role().trim() : null;

    }

    private static boolean containsPipe(String... vals) {
        for (String v : vals) if (v != null && v.contains("|")) return true;
        return false;
    }

    private static void doRegisterStudent(ClientConnection conn, BufferedReader keyboard) throws Exception {
        System.out.println();
        System.out.println("== Register student ==");
        System.out.print("Student number: ");
        int number = Integer.parseInt(keyboard.readLine());
        System.out.print("Name: ");
        String name = keyboard.readLine();
        System.out.print("Email: ");
        String email = keyboard.readLine();
        System.out.print("Password: ");
        String password = keyboard.readLine();

        if (containsPipe(name, email, password)) {
            System.out.println("[CLIENT] Fields cannot contain the '|' character.");
            return;
        }

        String req = ClientServerProtocol.buildRegisterStudentRequest(number, name, email, password);
        conn.sendLine(req);

        String resp;
        try {
            resp = conn.safeReadLine();
        } catch (Exception e) {
            System.err.println("[CLIENT] Error reading register-student response (server may have died / switched): " + e.getMessage());
            System.err.println("[CLIENT] Please try again in a few seconds.");
            return;
        }

        ClientServerProtocol.RegisterResponse rr = ClientServerProtocol.parseRegisterResponse(resp);

        if (rr == null) {
            System.out.println("[CLIENT] Invalid response from server: " + resp);
        } else if (rr.success()) {
            System.out.println("[CLIENT] Registration OK: " + rr.message());
        } else {
            System.out.println("[CLIENT] Registration failed: " + rr.message());
        }
    }

    private static void doRegisterTeacher(ClientConnection conn, BufferedReader keyboard) throws Exception {
        System.out.println();
        System.out.println("== Register teacher ==");
        System.out.print("Name: ");
        String name = keyboard.readLine();
        System.out.print("Email: ");
        String email = keyboard.readLine();
        System.out.print("Password: ");
        String password = keyboard.readLine();
        System.out.print("Teacher code (the one shared by teachers): ");
        String teacherCode = keyboard.readLine();

        if (containsPipe(name, email, password, teacherCode)) {
            System.out.println("[CLIENT] Fields cannot contain the '|' character.");
            return;
        }

        String req = ClientServerProtocol.buildRegisterTeacherRequest(name, email, password, teacherCode);
        conn.sendLine(req);

        String resp;
        try {
            resp = conn.safeReadLine();
        } catch (Exception e) {
            System.err.println("[CLIENT] Error reading register-teacher response (server may have died / switched): " + e.getMessage());
            System.err.println("[CLIENT] Please try again in a few seconds.");
            return;
        }

        ClientServerProtocol.RegisterResponse rr = ClientServerProtocol.parseRegisterResponse(resp);

        if (rr == null) {
            System.out.println("[CLIENT] Invalid response from server: " + resp);
        } else if (rr.success()) {
            System.out.println("[CLIENT] Registration OK: " + rr.message());
        } else {
            System.out.println("[CLIENT] Registration failed: " + rr.message());
        }
    }

    private static void postLoginLoop(ClientConnection conn, BufferedReader keyboard, String role) throws Exception {
        boolean isTeacher = "TEACHER".equalsIgnoreCase(role);
        System.out.println();
        System.out.println("You are now logged in. Type commands (or 'logout' to return to menu):");

        String line;
        while (true) {
            // print role-specific menu
            System.out.println();
            System.out.println("=== Menu (" + (isTeacher ? "Teacher" : "Student") + ") ===");
            if (isTeacher) {
                System.out.println("1) Create question");
                System.out.println("2) List my questions");
                System.out.println("3) View results");
                System.out.println("4) Export results");
                System.out.println("5) Export all results");
                System.out.println("6) Edit profile");
                System.out.println("7) Logout");
            } else {
                System.out.println("1) Answer question");
                System.out.println("2) List my answers");
                System.out.println("3) Edit profile");
                System.out.println("4) Logout");
            }
            System.out.print("Option or command: ");

            line = keyboard.readLine();
            if (line == null) {
                System.out.println("[CLIENT] Input closed.");
                conn.sendLine("LOGOUT");
                return;
            }
            line = line.trim();
            if (line.isEmpty()) continue;

            // map numeric choices to actual commands
            String toSend = line;
            if (isTeacher) {
                switch (line) {
                    case "1" -> toSend = "CREATE_QUESTION";
                    case "2" -> toSend = "LIST_MY_QUESTIONS";
                    case "3" -> toSend = "VIEW_RESULTS";
                    case "4" -> toSend = "EXPORT_RESULTS";
                    case "5" -> toSend = "EXPORT_ALL_RESULTS";
                    case "6" -> toSend = "EDIT_PROFILE";
                    case "7" -> {
                        System.out.println("[CLIENT] Logging out...");
                        conn.sendLine("LOGOUT");
                        return;
                    }
                }
            } else {
                switch (line) {
                    case "1" -> toSend = "ANSWER_QUESTION";
                    case "2" -> toSend = "LIST_MY_ANSWERS";
                    case "3" -> toSend = "EDIT_PROFILE";
                    case "4" -> {
                        System.out.println("[CLIENT] Logging out...");
                        conn.sendLine("LOGOUT");
                        return;
                    }
                }
            }

            // send the command typed by the user (or mapped numeric)
            conn.sendLine(toSend);

            try {
                String serverResp = conn.safeReadLine();
                if (serverResp == null) {
                    System.out.println("[CLIENT] Server closed connection.");
                    return;
                }

                String trimmed = serverResp.trim();

                // 1) question block special handling
                if (trimmed.startsWith("QUESTION_BLOCK ")) {
                    handleQuestionBlock(serverResp, conn, keyboard);
                    continue;
                }

                if (trimmed.startsWith("RESULT_BLOCK ")) {
                    handleResultBlock(serverResp);
                    continue;
                }

                // 1.5) generic BLOCK (multi-line info, no extra input)
                if (trimmed.startsWith("BLOCK ")) {
                    String payload = trimmed.substring("BLOCK ".length());
                    payload = payload.replace("\\n", "\n");

                    String[] linesOut = payload.split("\n");
                    for (String l : linesOut) {
                        if (!l.isEmpty()) {
                            System.out.println(l);
                        }
                    }
                    continue;
                }

                // 2) generic prompts (including "PROMPT Access code:")
                System.out.println("SERVER: " + serverResp);
                if (trimmed.startsWith("PROMPT ") || trimmed.endsWith(":")) {
                    while (true) {
                        String userInput = keyboard.readLine();
                        if (userInput == null) return;
                        conn.sendLine(userInput);

                        String resp = conn.safeReadLine();
                        if (resp == null) {
                            System.out.println("[CLIENT] Server closed connection.");
                            return;
                        }

                        String t = resp.trim();

                        // if the server now sends a question block, handle it specially
                        if (t.startsWith("QUESTION_BLOCK ")) {
                            handleQuestionBlock(resp, conn, keyboard);
                            break; // done with this prompt flow
                        }

                        if (t.startsWith("RESULT_BLOCK ")) {
                            handleResultBlock(resp);
                            break; // done with this prompt flow
                        }

                        System.out.println("SERVER: " + resp);

                        // stay in this inner-loop only while server keeps prompting
                        if (!(t.startsWith("PROMPT ") || t.endsWith(":"))) {
                            break;
                        }
                    }
                    continue;
                }
            } catch (Exception e) {
                System.err.println("[CLIENT] Error reading from server: " + e.getMessage());
                return;
            }
        }
    }

    private static void handleResultBlock(String serverResp) {
        String trimmed = serverResp.trim();
        String payload = trimmed.substring("RESULT_BLOCK ".length());

        // decode "\n" back to real newlines
        payload = payload.replace("\\n", "\n");

        String[] lines = payload.split("\n");
        for (String l : lines) {
            if (!l.isEmpty()) {
                System.out.println(l);
            }
        }
    }

    private static void handleQuestionBlock(String serverResp,
                                            ClientConnection conn,
                                            BufferedReader keyboard) throws Exception {
        String trimmed = serverResp.trim();
        String payload = trimmed.substring("QUESTION_BLOCK ".length());

        // decode "\n" back to real newlines
        payload = payload.replace("\\n", "\n");

        String[] lines = payload.split("\n");
        for (String l : lines) {
            if (!l.isEmpty()) {
                System.out.println(l);
            }
        }

        System.out.print("Enter option code: ");
        String ans = keyboard.readLine();
        if (ans == null) {
            return;
        }
        conn.sendLine(ans);

        String finalResp = conn.safeReadLine();
        if (finalResp == null) {
            System.out.println("[CLIENT] Server closed connection.");
            return;
        }
        System.out.println("SERVER: " + finalResp);
    }
}