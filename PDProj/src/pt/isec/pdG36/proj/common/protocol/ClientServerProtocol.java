package pt.isec.pdG36.proj.common.protocol;

public final class ClientServerProtocol {

    private ClientServerProtocol() {}

    private static final String LOGIN = "LOGIN";
    private static final String LOGIN_OK = "LOGIN_OK";
    private static final String LOGIN_FAIL = "LOGIN_FAIL";
    private static final String ERROR = "ERROR";

    // Builders
    public static String buildLoginRequest(String username, String password) {
        return LOGIN + " " + username + " " + password;
    }

    public static String buildLoginOk(String role, String message) {
        return LOGIN_OK + " " + role + " " + (message == null ? "" : message);
    }

    public static String buildLoginFail(String message) {
        return LOGIN_FAIL + " " + (message == null ? "" : message);
    }

    public static String buildError(String message) {
        return ERROR + " " + (message == null ? "" : message);
    }

    // Records
    public record LoginResponse(boolean success, String role, String message) {}
    public record LoginRequest(String username, String password) {}

    // Parsers
    public static LoginResponse parseLoginResponse(String line) {
        if (line == null) return null;
        String trimmed = line.trim();
        if (trimmed.isEmpty()) return null;
        String[] parts = trimmed.split("\\s+", 3); // limit 3 to preserve message
        if (LOGIN_OK.equals(parts[0])) {
            String role = parts.length >= 2 ? parts[1] : null;
            String msg = parts.length == 3 ? parts[2] : "";
            return new LoginResponse(true, role, msg);
        } else if (LOGIN_FAIL.equals(parts[0])) {
            String msg = parts.length >= 2 ? parts[1] : "";
            if (parts.length > 2) msg = parts[1] + " " + parts[2];
            return new LoginResponse(false, null, msg);
        } else if (ERROR.equals(parts[0])) {
            String msg = parts.length >= 2 ? parts[1] : "";
            if (parts.length > 2) msg = parts[1] + " " + parts[2];
            return new LoginResponse(false, null, "ERROR: " + msg);
        }
        return null;
    }

    public static LoginRequest parseLoginRequest(String line) {
        if (line == null) return null;
        String trimmed = line.trim();
        if (trimmed.isEmpty()) return null;
        String[] parts = trimmed.split("\\s+", 3);
        if (parts.length >= 3 && LOGIN.equals(parts[0])) {
            return new LoginRequest(parts[1], parts[2]);
        }
        return null;
    }
}
