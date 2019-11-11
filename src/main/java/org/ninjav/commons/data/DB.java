package org.ninjav.commons.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DB {

    public static final BiConsumer<Connection, String> update = (con, query) -> {
        try {
            final Statement stmt = con.createStatement();
            stmt.executeUpdate(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    };

    private static Connection connect(String dbDriver, String dbUrl, String dbUser, String dbPassword) {
        try {
            Class.forName(dbDriver);
            return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
        } catch (SQLException e) {
            throw new CannotConnectToDB(e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new CannotLoadDBDriver(e.getMessage());
        }
    }

    public static void use(String dbDriver, String dbUrl, String dbUser, String dbPassword,
                           Consumer<Connection> block) {
        Connection con = null;
        try {
            con = connect(dbDriver, dbUrl, dbUser, dbPassword);
            block.accept(con);
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
            }
        }
    }

    public static class CannotConnectToDB extends RuntimeException {

        public CannotConnectToDB(String message) {
            super(message);
        }
    }

    public static class CannotLoadDBDriver extends RuntimeException {
        public CannotLoadDBDriver(String message) {
            super(message);
        }
    }
}
