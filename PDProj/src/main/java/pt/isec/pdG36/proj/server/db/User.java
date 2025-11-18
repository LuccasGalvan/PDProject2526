package pt.isec.pdG36.proj.server.db;

public record User(
        long id,
        String role,           // "TEACHER" or "STUDENT"
        Integer studentNumber, // null for teacher
        String name,
        String email
) {}