package pt.isec.pdG36.proj.common.protocol;

public final class DirectoryProtocol {

    private DirectoryProtocol() {}

    // Build (servers & clients

    public static String buildRegister(int clientPort, int dbPort) {
        return "REGISTER " + clientPort + " " + dbPort;
    }

    public static String buildHeartbeat(int clientPort, int dbPort, long dbVersion) {
        return "HEARTBEAT " + clientPort + " " + dbPort + " " + dbVersion;
    }

    public static String buildUnregister(int clientPort, int dbPort) {
        return "UNREGISTER " + clientPort + " " + dbPort;
    }

    public static String buildGetServer() {
        return "GET_SERVER";
    }

    public static String buildServerReply(String ip, int clientPort) {
        return "SERVER " + ip + " " + clientPort;
    }

    public static String buildNoServerReply() {
        return "NO_SERVER";
    }

    public static String buildPrimaryReply(String ip, int dbPort) {
        return "PRIMARY " + ip + " " + dbPort;
    }

    // Parse helpers for replies

    public record PrimaryInfo(String ip, int dbPort) {}
    public record ServerInfoResp(String ip, int clientPort) {}

    public static PrimaryInfo parsePrimary(String msg) {
        String[] p = msg.trim().split("\\s+");
        if (p.length == 3 && "PRIMARY".equals(p[0])) {
            return new PrimaryInfo(p[1], Integer.parseInt(p[2]));
        }
        return null;
    }

    public static ServerInfoResp parseServerReply(String msg) {
        String[] p = msg.trim().split("\\s+");
        if (p.length == 3 && "SERVER".equals(p[0])) {
            return new ServerInfoResp(p[1], Integer.parseInt(p[2]));
        }
        return null;
    }

    public static boolean isNoServer(String msg) {
        return "NO_SERVER".equals(msg.trim());
    }
}

