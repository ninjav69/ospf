package org.ninjav.brokerbase;

import org.ninjav.commons.data.DB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class Branch {

    public static void dropBranch(Connection con) {
        DB.update.accept(con, "DROP TABLE B_BROKER");
    }

    public static void createBranch(Connection con) {
        DB.update.accept(con, "CREATE TABLE B_BRANCH (\n" +
                "        PROV_CODE varchar2(80),\n" +
                "        PROV_DESCRIPT varchar2(80),\n" +
                "        PROV_MANAGER varchar2(80),\n" +
                "        REGN_CODE varchar2(80),\n" +
                "        REGN_DESCRIPT varchar2(80),\n" +
                "        REGN_MANAGER varchar2(80),\n" +
                "        BRAN_CODE varchar2(80),\n" +
                "        BRAN_DESCRIPT varchar2(80)\n" +
                ")\n");
    }

    static PreparedStatement insertStmt;

    public static void insertBranch(final Connection con, final List<String> cells) {
        try {
            if (insertStmt == null) {
                insertStmt = con.prepareStatement(
                        "INSERT INTO B_BRANCH (\n" +
                                "        PROV_CODE,\n" +
                                "        PROV_DESCRIPT,\n" +
                                "        PROV_MANAGER,\n" +
                                "        REGN_CODE,\n" +
                                "        REGN_DESCRIPT,\n" +
                                "        REGN_MANAGER,\n" +
                                "        BRAN_CODE,\n" +
                                "        BRAN_DESCRIPT\n" +
                                ") VALUES (\n" +
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
