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

        // Login flow
        System.out.print("Username: ");
        String username = keyboard.readLine();
        System.out.print("Password: ");
        String password = keyboard.readLine();

        conn.sendLine(ClientServerProtocol.buildLoginRequest(username, password));

        String respLine = null;
        try {
            respLine = conn.readLine();
        } catch (Exception e) {
            System.err.println("[CLIENT] Error reading login response: " + e.getMessage());
            conn.close();
            return;
        }

        ClientServerProtocol.LoginResponse lr = ClientServerProtocol.parseLoginResponse(respLine);
        if (lr == null) {
            System.err.println("[CLIENT] Invalid response from server.");
            conn.close();
            return;
        }

        if (!lr.success()) {
            System.err.println("[CLIENT] Login failed: " + lr.message());
            conn.close();
            return;
        }

        System.out.println("[CLIENT] Login successful. Role: " + lr.role() + " | " + lr.message());
        System.out.println("Type something to send (or 'quit' to exit):");

        String line;
        while ((line = keyboard.readLine()) != null) {
            if ("quit".equalsIgnoreCase(line)) break;
            conn.sendLine(line);
            try {
                String serverResp = conn.readLine();
                System.out.println("SERVER: " + serverResp);
            } catch (Exception e) {
                System.err.println("[CLIENT] Error reading from server: " + e.getMessage());
                break;
            }
        }

        conn.close();
    }
}
