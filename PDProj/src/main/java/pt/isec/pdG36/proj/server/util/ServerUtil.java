package pt.isec.pdG36.proj.server.util;

public class ServerUtil {
    public static String escapeSqlLiteral(String value) {
        if (value == null) return "";
        return value.replace("'", "''");
    }

    public static String toCsvField(String value) {
        if (value == null)
            return "\"\"";
        String v = value.replace("\"", "\"\""); // escape double-quotes
        return "\"" + v + "\"";
    }

    public static String[] splitCommandLine(String line) {
        if (line == null) return new String[0];
        String trimmed = line.trim();
        if (trimmed.isEmpty()) return new String[0];

        int spaceIdx = trimmed.indexOf(' ');
        String cmd = (spaceIdx == -1) ? trimmed.toUpperCase() : trimmed.substring(0, spaceIdx).toUpperCase();

        if ("REGISTER_STUDENT".equalsIgnoreCase(cmd) || "REGISTER_TEACHER".equalsIgnoreCase(cmd)) {
            if (spaceIdx == -1) {
                // command without payload
                return new String[]{cmd};
            }
            String payload = trimmed.substring(spaceIdx + 1);
            // split preserving empty fields; trim each field
            String[] fields = payload.split("\\|", -1);
            String[] parts = new String[fields.length + 1];
            parts[0] = cmd;
            for (int i = 0; i < fields.length; i++) {
                parts[i + 1] = fields[i] == null ? "" : fields[i].trim();
            }
            return parts;
        } else {
            // Default: split by whitespace. Keep simple tokenization for other commands.
            // Use limit to preserve trailing message when relevant (e.g., LOGIN responses parsing handled elsewhere)
            return trimmed.split("\\s+");
        }
    }
}
