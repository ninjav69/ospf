package org.ninjav.brokerbase;

import org.ninjav.commons.data.DB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class Adviser {
    public static void dropAdviser(Connection con) {
        DB.update.accept(con, "DROP TABLE B_AD");
    }

    public static void createAdviser(Connection con) {
        DB.update.accept(con, "CREATE TABLE B_AD (\n" +
                "        ID varchar2(80),\n" +
                "        RELATION_ID varchar2(20),\n" +
                "        CODE_1 varchar2(80),\n" +
                "        TITLE varchar2(80),\n" +
                "        INITIALS varchar2(80),\n" +
                "        SURANME varchar2(80),\n" +
                "        PRODUCT_SUPPL varchar2(80),\n" +
                "        PRODUCT varchar2(80),\n" +
                "        ACCREDITED_Y_N varchar2(80),\n" +
                "        ACCRED_DATE varchar2(80),\n" +
                "        ACC_END_DATE varchar2(80),\n" +
                "        PASS_FAIL_P_F varchar2(80),\n" +
                "        PERCENTAGE varchar2(80),\n" +
                "        PRODUCT_SUPPL2 varchar2(80),\n" +
                "        EXAM_DATE_WRITE varchar2(80),\n" +
                "        ATTEMPTS varchar2(80)\n" +
                ")\n");
    }

    static PreparedStatement insertStmt;

    public static void insertAdviser(Connection con, List<String> cells) {
        try {
            if (insertStmt == null) {
                insertStmt = con.prepareStatement(
                        "INSERT INTO B_AD (\n" +
                                "        ID,\n" +
                                "        RELATION_ID,\n" +
                                "        CODE_1,\n" +
                                "        TITLE,\n" +
                                "        INITIALS,\n" +
                                "        SURANME,\n" +
                                "        PRODUCT_SUPPL,\n" +
                                "        PRODUCT,\n" +
                                "        ACCREDITED_Y_N,\n" +
                                "        ACCRED_DATE,\n" +
                                "        ACC_END_DATE,\n" +
                                "        PASS_FAIL_P_F,\n" +
                                "        PERCENTAGE,\n" +
                                "        PRODUCT_SUPPL2,\n" +
                                "        EXAM_DATE_WRITE,\n" +
                                "        ATTEMPTS\n" +
                                ") VALUES (\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?\n" +
                                ")\n");
            }

            for (int i = 0; i < cells.size(); i++) {
                insertStmt.setString(i + 1, cells.get(i));
            }

            insertStmt.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
