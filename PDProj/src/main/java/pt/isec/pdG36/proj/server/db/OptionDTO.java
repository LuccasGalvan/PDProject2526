package pt.isec.pdG36.proj.server.db;

public record OptionDTO(
        long id,
        long questionId,
        String code,
        String text,
        boolean isCorrect
) {
}
