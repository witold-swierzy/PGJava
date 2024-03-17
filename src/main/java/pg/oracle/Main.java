package pg.oracle;

import oracle.jdbc.pool.OracleDataSource;
import oracle.pg.rdbms.GraphServer;

import java.sql.*;
import oracle.pg.rdbms.GraphServer;
import oracle.pgx.api.*;

public class Main {
    private static String dbHost    = "130.61.110.25";
    private static String pgxHost   = "130.61.73.166";
    private static int dbPort       = 1521;
    private static int pgxPort      = 7007;
    private static String dbService = "demo.wssubnet0001.wsvcn0001.oraclevcn.com";
    private static String username  = "oradev";
    private static String password  = "WELcome__1234";
    private static String pgName = "CUSTOMER_GRAPH";
    public static void SQLPGDemo() {
        try {
            OracleDataSource ds = new OracleDataSource();
            ds.setDriverType("thin");
            ds.setServerName(dbHost);
            ds.setServiceName(dbService);
            ds.setPortNumber(dbPort);
            ds.setUser(username);
            ds.setPassword(password);
            Connection con = ds.getConnection();
            System.out.println("Connected to the database");
            System.out.println("(Re)creating property graph "+pgName);
            Statement stmt = con.createStatement();
            stmt.execute("CREATE OR REPLACE PROPERTY GRAPH "+pgName+" VERTEX TABLES (\n" +
                    "    customer\n" +
                    "  , account\n" +
                    "  , merchant\n" +
                    "  )\n" +
                    "  EDGE TABLES (\n" +
                    "    account as account_edge\n" +
                    "      SOURCE KEY(id) REFERENCES account (id)\n" +
                    "      DESTINATION KEY(customer_id) REFERENCES customer (id)\n" +
                    "      LABEL owned_by PROPERTIES (id)\n" +
                    "  , parent_of as parent_of_edge \n" +
                    "      SOURCE KEY(customer_id_parent) REFERENCES customer (id)\n" +
                    "      DESTINATION KEY(customer_id_child) REFERENCES customer (id)\n" +
                    "  , purchased as puchased_edge \n" +
                    "      SOURCE KEY(account_id) REFERENCES account (id)\n" +
                    "      DESTINATION KEY(merchant_id) REFERENCES merchant (id)\n" +
                    "  , transfer as transfer_edge \n" +
                    "      SOURCE KEY(account_id_from) REFERENCES account (id)\n" +
                    "      DESTINATION KEY(account_id_to) REFERENCES account (id)\n" +
                    "  ) ");
            System.out.println("Graph (re)created succesfully.");
            ResultSet rset = stmt.executeQuery("SELECT account_no\n" +
                    "FROM GRAPH_TABLE ( CUSTOMER_GRAPH MATCH (v1)-[transfer_edge]->{1,2}(v1)\n" +
                    "columns (v1.account_no as account_no))");
            while (rset.next()) {
                System.out.println(rset.getString(1));
            }
            con.close();
        }
        catch (Exception e) {e.printStackTrace();}
    }

    public static void PGXDemo() {
        try {
            ServerInstance si = GraphServer.getInstance("http://"+pgxHost+":"+pgxPort,username,password.toCharArray());
            PgxSession ses = si.createSession("my-session");
            System.out.println("Connected to graph server");
            PgxGraph graph = ses.readGraphByName(username.toUpperCase(), pgName, GraphSource.PG_SQL);
            System.out.println("Graph loaded into Property Graph Server");
            PgqlResultSet rset = graph.queryPgql("SELECT a1.account_no    AS a1_account\n" +
                                "    , t1.transfer_date AS t1_date\n" +
                                "     , t1.amount        AS t1_amount\n" +
                                "     , a2.account_no    AS a2_account\n" +
                                "     , t2.transfer_date AS t2_date\n" +
                                "     , t2.amount        AS t2_amount\n" +
                                "FROM MATCH (a1)-[t1:transfer_edge]->(a2)-[t2:transfer_edge]->(a1)\n" +
                                "WHERE t1.transfer_date < t2.transfer_date");
            while (rset.next()) {
                System.out.println(rset.getString(1));
            }
        }
        catch (Exception e) {e.printStackTrace();}
    }
    public static void main(String[] args) {
        SQLPGDemo();
        PGXDemo();
    }
}