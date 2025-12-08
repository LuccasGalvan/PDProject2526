package pt.isec.pdG36.proj.server.db;

public record StudentAnswerDTO(
        long questionId,
        String statement,
        String startTime,
        String endTime,
        String optionCode,
        boolean correct
) {
}
