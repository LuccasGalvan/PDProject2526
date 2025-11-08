package pt.isec.pdG36.proj.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;

public class ClientMain {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java ClientMain <dirIP> <dirUdpPort>");
            return;
        }

        InetAddress dirAddr = InetAddress.getByName(args[0]);
        int dirPort = Integer.parseInt(args[1]);

        ClientConnection conn = new ClientConnection(dirAddr, dirPort);
        if (!conn.connectToPrimary()) {
            System.err.println("Exiting (no server).");
            return;
        }

        // TODO: authentication / registration protocol
        // For now, tiny echo-style loop
        BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));
        String line;
        System.out.println("Type something (or 'quit'):");
        while ((line = keyboard.readLine()) != null) {
            if (line.equalsIgnoreCase("quit")) break;
            conn.sendLine(line);
            // In future version: handle async notifications in a separate thread
        }

        conn.close();
    }
}
