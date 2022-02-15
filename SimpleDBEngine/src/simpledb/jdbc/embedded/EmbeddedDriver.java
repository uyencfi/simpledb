package simpledb.jdbc.embedded;

import java.sql.SQLException;
import java.util.Properties;

import simpledb.jdbc.DriverAdapter;
import simpledb.server.SimpleDB;

/**
 * The RMI server-side implementation of RemoteDriver.
 * @author Edward Sciore
 */

public class EmbeddedDriver extends DriverAdapter {   
   /**
    * Creates a new RemoteConnectionImpl object and 
    * returns it.
    * @see simpledb.jdbc.network.RemoteDriver#connect()
    */
   public EmbeddedConnection connect(String url, Properties p) throws SQLException {
      String dbname = url.replace("jdbc:simpledb:", "");
      SimpleDB db = new SimpleDB(dbname);
      return new EmbeddedConnection(db);
   }
}

