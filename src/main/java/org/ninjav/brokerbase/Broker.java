package org.ninjav.brokerbase;

import org.ninjav.commons.data.DB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class Broker {

    public static void dropBroker(final Connection con) {
        DB.update.accept(con, "DROP TABLE B_BROKER");
    }

    public static void createBroker(Connection con) {
        DB.update.accept(con, "CREATE TABLE B_BROKER (\n" +
                "        BRANCH varchar2(80),\n" +
                "        ADVISER_CODE varchar2(80),\n" +
                "        ADVISER_ID varchar2(80),\n" +
                "        ADVISER_TITLE varchar2(80),\n" +
                "        ADVISER_INITS varchar(80),\n" +
                "        ADVISER_FST_NAME varchar2(80),\n" +
                "        ADVISER_PRF_NAME varchar2(80),\n" +
                "        ADVISER_SURNAME varchar2(80),\n" +
                "        ID_NUMBER varchar2(80),\n" +
                "        BROKER_HOUSE varchar2(80),\n" +
                "        BROKER_CODE varchar2(80),\n" +
                "        TITLE varchar2(80),\n" +
                "        INITIALS varchar2(80),\n" +
                "        FIRST_NAME varchar2(80),\n" +
                "        NICK_NAME varchar2(80),\n" +
                "        SURNAME varchar2(80),\n" +
                "        CATEGORY varchar2(80),\n" +
                "        GENDER varchar2(80),\n" +
                "        DOB varchar2(80),\n" +
                "        LANGUAGE varchar2(80),\n" +
                "        MEB_ACC_PRD_1 varchar2(80),\n" +
                "        MDS_ACC_STS_1 varchar2(80),\n" +
                "        MEB_ACC_PRD_2 varchar2(80),\n" +
                "        MDS_ACC_STS_2 varchar2(80),\n" +
                "        MEB_ACC_PRD_3 varchar2(80),\n" +
                "        MDS_ACC_STS_3 varchar2(80),\n" +
                "        VAT_INDICAT varchar2(80),\n" +
                "        VAT_DATE varchar2(80),\n" +
                "        IN_SERVICE varchar2(80),\n" +
                "        REGION_DESC varchar2(80),\n" +
                "        REGION_CODE varchar2(80),\n" +
                "        OUT_OF_SERVICE varchar2(80),\n" +
                "        ROLE varchar2(80)\n" +
                ")\n");
    }

    static PreparedStatement insertBrokerStmt;

    public static void insertBroker(final Connection con, final List<String> cells) {
        try {
            if (insertBrokerStmt == null) {
                insertBrokerStmt = con.prepareStatement(
                        "INSERT INTO B_BROKER (\n" +
                                "        BRANCH,\n" +
                                "        ADVISER_CODE,\n" +
                                "        ADVISER_ID,\n" +
                                "        ADVISER_TITLE,\n" +
                                "        ADVISER_INITS,\n" +
                                "        ADVISER_FST_NAME,\n" +
                                "        ADVISER_PRF_NAME,\n" +
                                "        ADVISER_SURNAME,\n" +
                                "        ID_NUMBER,\n" +
                                "        BROKER_HOUSE,\n" +
                                "        BROKER_CODE,\n" +
                                "        TITLE,\n" +
                                "        INITIALS,\n" +
                                "        FIRST_NAME,\n" +
                                "        NICK_NAME,\n" +
                                "        SURNAME,\n" +
                                "        CATEGORY,\n" +
                                "        GENDER,\n" +
                                "        DOB,\n" +
                                "        LANGUAGE,\n" +
                                "        MEB_ACC_PRD_1,\n" +
                                "        MDS_ACC_STS_1,\n" +
                                "        MEB_ACC_PRD_2,\n" +
                                "        MDS_ACC_STS_2,\n" +
                                "        MEB_ACC_PRD_3,\n" +
                                "        MDS_ACC_STS_3,\n" +
                                "        VAT_INDICAT,\n" +
                                "        VAT_DATE,\n" +
                                "        IN_SERVICE,\n" +
                                "        REGION_DESC,\n" +
                                "        REGION_CODE,\n" +
                                "        OUT_OF_SERVICE,\n" +
                                "        ROLE\n" +
                                ") VALUES (\n        " +
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
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?\n" +
                                ")\n");
            }

            for (int i = 0; i < cells.size(); i++) {
                insertBrokerStmt.setString(i + 1, cells.get(i));
            }

            insertBrokerStmt.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
