package org.ninjav.broker;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.ninjav.data.DB;

public class BrokerData {

    public static void dropBrokerHouse(final Connection con) {
        DB.update.accept(con, "DROP TABLE B_BH");
    }

    public static void createBrokerHouse(final Connection con) {
        DB.update.accept(con, "CREATE TABLE B_BH (\n" +
                "        BROKER_HOUSE_CODE varchar2(20),\n" +
                "        BROKER_HOUSE_NAME varchar2(80),\n" +
                "        VAT_INDICAT varchar2(80),\n" +
                "        VAT_DATE varchar2(80),\n" +
                "        COMP_REG_NUMBER varchar2(80),\n" +
                "        FSP_REGISTERED varchar2(80)\n" +
                ")\n");
    }

    static PreparedStatement insertBrokerhouseStmt;

    public static void insertBrokerHouse(final Connection con, final List<String> cells) {
        try {
            if (insertBrokerhouseStmt == null) {
                insertBrokerhouseStmt = con.prepareStatement(
                        "INSERT INTO B_BH (\n" +
                                "        BROKER_HOUSE_CODE,\n" +
                                "        BROKER_HOUSE_NAME,\n" +
                                "        VAT_INDICAT,\n" +
                                "        VAT_DATE,\n" +
                                "        COMP_REG_NUMBER,\n" +
                                "        FSP_REGISTERED\n" +
                                ") VALUES (\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?,\n" +
                                "        ?\n" +
                                ")\n"
                );
            }

            cells.set(2, format.apply(cells.get(2)));
            for (int i = 0; i < cells.size(); i++) {
                insertBrokerhouseStmt.setString(i + 1, cells.get(i));
            }

            insertBrokerhouseStmt.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

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

    static PreparedStatement insertBranchStmt;

    public static void insertBranch(final Connection con, final List<String> cells) {
        try {
            if (insertBranchStmt == null) {
                insertBranchStmt = con.prepareStatement(
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
                insertBranchStmt.setString(i + 1, cells.get(i));
            }

            insertBranchStmt.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

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

    static PreparedStatement insertAdviserStmt;

    public static void insertAdviser(Connection con, List<String> cells) {
        try {
            if (insertAdviserStmt == null) {
                insertAdviserStmt = con.prepareStatement(
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
                insertAdviserStmt.setString(i + 1, cells.get(i));
            }

            insertAdviserStmt.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Function<String, String> format = str -> {
        final Pattern p = Pattern.compile("([^\"]*)\".*");
        final Matcher m = p.matcher(str);
        return m.find() && m.groupCount() == 1 ? m.group(1) : str;
    };
}
