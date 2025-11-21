package pt.isec.pdG36.proj.client;

import pt.isec.pdG36.proj.common.protocol.ClientServerProtocol;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;

public class ClientMain {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java client.ClientMain <dirIP> <dirUdpPort>");
            return;
        }

        InetAddress dirAddr = InetAddress.getByName(args[0]);
        int dirPort = Integer.parseInt(args[1]);

        ClientConnection conn = new ClientConnection(dirAddr, dirPort);

        if (!conn.connectToPrimary()) {
            System.err.println("[CLIENT] Could not connect to any server. Exiting.");
            return;
        }

        BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            System.out.println();
            System.out.println("=== QUIZ SYSTEM ===");
            System.out.println("1) Login");
            System.out.println("2) Register student");
            System.out.println("3) Register teacher");
            System.out.println("4) Quit");
            System.out.print("Option: ");

            String option = keyboard.readLine();
            if (option == null) break;
            option = option.trim();

            switch (option) {
                case "1" -> {
                    if (doLogin(conn, keyboard)) {
                        // if login OK, go into post-login loop and then return to menu when done
                        postLoginLoop(conn, keyboard);
                    }
                }
                case "2" -> doRegisterStudent(conn, keyboard);
                case "3" -> doRegisterTeacher(conn, keyboard);
                case "4" -> {
                    conn.close();
                    return;
                }
                default -> System.out.println("Invalid option. Try again.");
            }
        }

        conn.close();
    }

    private static boolean doLogin(ClientConnection conn, BufferedReader keyboard) throws Exception {
        System.out.println();
        System.out.println("== Login ==");
        System.out.print("Email: ");
        String email = keyboard.readLine();
        System.out.print("Password: ");
        String password = keyboard.readLine();

        conn.sendLine(ClientServerProtocol.buildLoginRequest(email, password));

        String respLine;
        try {
            respLine = conn.readLine();
        } catch (Exception e) {
            System.err.println("[CLIENT] Error reading login response: " + e.getMessage());
            return false;
        }

        ClientServerProtocol.LoginResponse lr = ClientServerProtocol.parseLoginResponse(respLine);
        if (lr == null) {
            System.err.println("[CLIENT] Invalid response from server.");
            return false;
        }

        if (!lr.success()) {
            System.err.println("[CLIENT] Login failed: " + lr.message());
            return false;
        }

        System.out.println("[CLIENT] Login successful. Role: " + lr.role() + " | " + lr.message());
        return true;
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

        String req = ClientServerProtocol.buildRegisterStudentRequest(number, name, email, password);
        conn.sendLine(req);

        String resp = conn.readLine();
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

        // later you can hash this on the client if you want; for now send as-is
        String req = ClientServerProtocol.buildRegisterTeacherRequest(name, email, password, teacherCode);
        conn.sendLine(req);

        String resp = conn.readLine();
        ClientServerProtocol.RegisterResponse rr = ClientServerProtocol.parseRegisterResponse(resp);

        if (rr == null) {
            System.out.println("[CLIENT] Invalid response from server: " + resp);
        } else if (rr.success()) {
            System.out.println("[CLIENT] Registration OK: " + rr.message());
        } else {
            System.out.println("[CLIENT] Registration failed: " + rr.message());
        }
    }

    private static void postLoginLoop(ClientConnection conn, BufferedReader keyboard) throws Exception {
        System.out.println();
        System.out.println("You are now logged in. Type commands (or 'logout' to return to menu):");

        String line;
        while ((line = keyboard.readLine()) != null) {
            if ("logout".equalsIgnoreCase(line)) {
                System.out.println("[CLIENT] Logging out...");
                // later we can send a LOGOUT command if you want the server to track sessions
                return;
            }

            conn.sendLine(line);
            try {
                String serverResp = conn.readLine();
                if (serverResp == null) {
                    System.out.println("[CLIENT] Server closed connection.");
                    return;
                }
                System.out.println("SERVER: " + serverResp);
            } catch (Exception e) {
                System.err.println("[CLIENT] Error reading from server: " + e.getMessage());
                return;
            }
        }
    }
}

