package pt.isec.pdG36.proj.server.db;

public record QuestionDTO(
        long id,
        long teacherId,
        String statement,
        String startTime,
        String endTime,
        String accessCode
) {
}
