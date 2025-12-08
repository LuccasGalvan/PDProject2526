package pt.isec.pdG36.proj.server.db;

public record AnswerResultRow(
        String timestamp,
        Integer studentNumber,
        String studentName,
        String studentEmail,
        String optionCode,
        boolean correct
) {
}
