package pt.isec.pdG36.proj.server.db;

public enum RegisterResult {
    OK,
    EMAIL_IN_USE,
    NUMBER_IN_USE,
    INVALID_TEACHER_CODE,
    DB_ERROR
}
