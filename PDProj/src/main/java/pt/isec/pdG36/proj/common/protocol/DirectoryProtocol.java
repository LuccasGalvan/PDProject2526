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

    public record HeartbeatInfo(int clientPort, int dbPort, long dbVersion) {}

    public static boolean isHeartbeat(String msg) {
        if (msg == null) return false;
        String trimmed = msg.trim();
        return trimmed.startsWith("HEARTBEAT ");
    }

    /**
     * Builds a HEARTBEAT that also carries a SQL statement for replication.
     * Format:
     *   HEARTBEAT <clientPort> <dbPort> <dbVersion> SQL <sql...>
     * Directory only cares about the first 4 tokens, so it’s safe.
     */
    public static String buildHeartbeatWithSql(int clientPort, int dbPort, long dbVersion, String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("sql must not be null/blank");
        }
        String sanitizedSql = sql.replace('\n', ' ').trim();
        return "HEARTBEAT " + clientPort + " " + dbPort + " " + dbVersion + " SQL " + sanitizedSql;
    }

    /**
     * Extracts the SQL part from a HEARTBEAT-with-SQL.
     * Returns null if there is no SQL part.
     */
    public static String extractSqlFromHeartbeat(String msg) {
        if (msg == null) return null;
        int idx = msg.indexOf(" SQL ");
        if (idx < 0) {
            return null;
        }
        return msg.substring(idx + " SQL ".length()).trim();
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

    public static HeartbeatInfo parseHeartbeat(String msg) {
        if (msg == null) {
            return null;
        }
        String[] p = msg.trim().split("\\s+");
        // Expected: HEARTBEAT <clientPort> <dbPort> <dbVersion> [SQL ...]
        if (p.length < 4) {
            return null;
        }
        if (!"HEARTBEAT".equals(p[0])) {
            return null;
        }
        try {
            int clientPort = Integer.parseInt(p[1]);
            int dbPort = Integer.parseInt(p[2]);
            long dbVersion = Long.parseLong(p[3]);
            return new HeartbeatInfo(clientPort, dbPort, dbVersion);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static boolean isNoServer(String msg) {
        return "NO_SERVER".equals(msg.trim());
    }
}

