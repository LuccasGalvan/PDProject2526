package pt.isec.pdG36.proj.server.util;

import pt.isec.pdG36.proj.server.db.AnswerResultRow;
import pt.isec.pdG36.proj.server.db.OptionDTO;
import pt.isec.pdG36.proj.server.db.QuestionDTO;

public record QuestionExportData(
        QuestionDTO question,
        java.util.List<OptionDTO> options,
        java.util.List<AnswerResultRow> answers
) {}
