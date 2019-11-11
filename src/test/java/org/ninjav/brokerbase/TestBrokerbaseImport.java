package org.ninjav.brokerbase;

import org.junit.Test;
import org.ninjav.commons.data.DB;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Consumer;

import static org.ninjav.commons.csv.CSV.*;

import static org.junit.Assert.assertTrue;

public class TestBrokerbaseImport {

    static final String DB_DRIVER = "org.sqlite.JDBC";
    static final String DB_URL = "jdbc:sqlite::memory:";
    static final String DB_USER = "";
    static final String DB_PASS = "";

    static final String brokerHouseCsv = "/home/ninjav/brokerbase/20191104/MEBFL1PF.TXT";
    static final String branchCsv = "/home/ninjav/brokerbase/20191104/MEBFL2PF.TXT";
    static final String adviserCsv = "/home/ninjav/brokerbase/20191104/MEBFL3PF.TXT";
    static final String brokerCsv = "/home/ninjav/brokerbase/20191104/MEBFLTPF.TXT";

    @Test
    public void testCsvMagicForAllBBFiles() {
        DB.use(DB_DRIVER, DB_URL, DB_USER, DB_PASS, con -> {
            //BrokerData.dropBrokerHouse(con);
            BrokerHouse.createBrokerHouse(con);

            withLinesIn(brokerHouseCsv, assertCellCount(6)
                    .andThen(b -> BrokerHouse.insertBrokerHouse(con, b)));

            try {
                final Statement stmt = con.createStatement();
                final ResultSet rs = stmt.executeQuery("select * from B_BH");
                while (rs.next()) {
                    System.out.println(rs.getString(1) + " "
                            + rs.getString(2) + " "
                            + rs.getString(3) + " "
                            + rs.getString(4) + " "
                            + rs.getString(5) + " "
                            + rs.getString(6));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            Broker.createBroker(con);

            withLinesIn(brokerCsv, assertCellCount(33)
                    .andThen(b -> Broker.insertBroker(con, b)));

            try {
                final Statement stmt = con.createStatement();
                final ResultSet rs = stmt.executeQuery("select * from B_BROKER");
                while (rs.next()) {
                    System.out.println(rs.getString(1) + " "
                            + rs.getString(2) + " "
                            + rs.getString(3) + " "
                            + rs.getString(4) + " "
                            + rs.getString(5) + " "
                            + rs.getString(6) + " "
                            + rs.getString(7) + " "
                            + rs.getString(8) + " "
                            + rs.getString(9) + " "
                            + rs.getString(10) + " "
                            + rs.getString(11) + " "
                            + rs.getString(12) + " "
                            + rs.getString(13) + " "
                            + rs.getString(14) + " "
                            + rs.getString(15) + " "
                            + rs.getString(16) + " "
                            + rs.getString(17) + " "
                            + rs.getString(18) + " "
                            + rs.getString(19) + " "
                            + rs.getString(20) + " "
                            + rs.getString(21) + " "
                            + rs.getString(22) + " "
                            + rs.getString(23) + " "
                            + rs.getString(24) + " "
                            + rs.getString(25) + " "
                            + rs.getString(26) + " "
                            + rs.getString(27) + " "
                            + rs.getString(28) + " "
                            + rs.getString(29) + " "
                            + rs.getString(30) + " "
                            + rs.getString(31) + " "
                            + rs.getString(32) + " "
                            + rs.getString(33));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            Branch.createBranch(con);

            withLinesIn(branchCsv, assertCellCount(8)
                    .andThen(b -> Branch.insertBranch(con, b)));

            try {
                final Statement stmt = con.createStatement();
                final ResultSet rs = stmt.executeQuery("select * from B_BRANCH");
                while (rs.next()) {
                    System.out.println(rs.getString(1) + " "
                            + rs.getString(2) + " "
                            + rs.getString(3) + " "
                            + rs.getString(4) + " "
                            + rs.getString(5) + " "
                            + rs.getString(6) + " "
                            + rs.getString(7) + " "
                            + rs.getString(8));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            Adviser.createAdviser(con);

            withLinesIn(adviserCsv, assertCellCount(16)
                    .andThen(a -> Adviser.insertAdviser(con, a)));

            try {
                final Statement stmt = con.createStatement();
                final ResultSet rs = stmt.executeQuery("select * from B_AD");
                while (rs.next()) {
                    System.out.println(rs.getString(1) + " "
                            + rs.getString(2) + " "
                            + rs.getString(3) + " "
                            + rs.getString(4) + " "
                            + rs.getString(5) + " "
                            + rs.getString(6) + " "
                            + rs.getString(7) + " "
                            + rs.getString(8) + " "
                            + rs.getString(9) + " "
                            + rs.getString(10) + " "
                            + rs.getString(11) + " "
                            + rs.getString(12) + " "
                            + rs.getString(13) + " "
                            + rs.getString(14) + " "
                            + rs.getString(15) + " "
                            + rs.getString(16));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Consumer<List<String>> assertCellCount(int expected) {
        return cells -> assertTrue(cells.size() == expected);
    }

}
