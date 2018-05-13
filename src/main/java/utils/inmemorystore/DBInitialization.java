package utils.inmemorystore;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
public class DBInitialization {
      public DBInitialization() throws Exception {
            this.initCreateTable();
      }
      public void initCreateTable() throws Exception {
            String dbURL = "jdbc:derby:derbydb;create=true";
            Connection conn = null;
            try {
                  Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
                  // Get a connection
                  conn = DriverManager.getConnection(dbURL);
                  DatabaseMetaData dbm = conn.getMetaData();
                  Statement stmt = conn.createStatement();
                  String tables[] = {"INVENTORY_FILE"};
                  for (int i = 0 ; i< tables.length; i++) {
                        ResultSet check = dbm.getTables(null,null,tables[i],null);
                        if (check.next()) {
                              stmt.executeUpdate("DROP TABLE "+ tables[i]);
                              stmt.executeUpdate("CREATE TABLE "+ tables[i] +" (item_id VARCHAR(50), store_id VARCHAR(50), quantity VARCHAR(50))");
                              System.out.print("table created");
                              }
                        else  {    
                              stmt.executeUpdate("CREATE TABLE "+ tables[i] +" (item_id VARCHAR(50), store_id VARCHAR(50), quantity VARCHAR(50))");
                              System.out.print("table created");
                        }
                  }
            } catch (java.sql.SQLException sqle) {
                  sqle.printStackTrace();
                  throw sqle;
            }
      }
}