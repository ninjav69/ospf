package org.ninjav.brokerbase;

import org.ninjav.commons.data.DB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BrokerHouse {
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

    private static final Function<String, String> format = str -> {
        final Pattern p = Pattern.compile("([^\"]*)\".*");
        final Matcher m = p.matcher(str);
        return m.find() && m.groupCount() == 1 ? m.group(1) : str;
    };
}
