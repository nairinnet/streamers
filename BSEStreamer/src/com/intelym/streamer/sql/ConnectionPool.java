 
package com.intelym.streamer.sql;

import com.intelym.logger.LoggerFactory;
import com.intelym.logger.QuickLogger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;



public class ConnectionPool {

    private static final QuickLogger mLog = LoggerFactory.getLogger(ConnectionPool.class);

    private BlockingQueue<Connection> pool;
    /** Maximum number of connections that the pool can have */
    private int maxPoolSize;
    /** Number of connections that should be created initially */
    private int initialPoolSize;
    /** Number of connections generated so far */
    private int currentPoolSize;

    private String dbUrl;
    private String dbUser;
    private String dbPassword;

    private static ConnectionPool connectionPool = null;
    
    
    private ConnectionPool(int maxPoolSize, int initialPoolSize, String url, String username,
                          String password, String driverClassName) throws ClassNotFoundException, SQLException {

        if( (initialPoolSize > maxPoolSize) || initialPoolSize<1 || maxPoolSize <1 ) {
            throw new IllegalArgumentException("Invalid pool size parameters");
        }

        // default max pool size to 10
        this.maxPoolSize = maxPoolSize > 0 ? maxPoolSize : 10;
        this.initialPoolSize = initialPoolSize;
        this.dbUrl = url;
        this.dbUser = username;
        this.dbPassword = password;
        this.pool = new LinkedBlockingQueue<>(maxPoolSize);

        initPooledConnections(driverClassName);

        if(pool.size() != initialPoolSize) {
            mLog.info("Initial sized pool creation failed. InitializedPoolSize={0}, initialPoolSize={1}");
                      
        }

    }

    public static ConnectionPool newInstance(int maxPoolSize, int initialPoolSize, String url, String username,
                          String password, String driverClassName) throws Exception{
        
        return new ConnectionPool(maxPoolSize, initialPoolSize, url, username, password, driverClassName);
    }
    
    
    
    private void initPooledConnections(String driverClassName)
            throws ClassNotFoundException, SQLException {

        // 1. Attempt to load the driver class
        Class.forName(driverClassName);

        // 2. Create and pool connections
        for(int i=0; i<initialPoolSize; i++) {
            openAndPoolConnection();
        }
    }

    private synchronized void openAndPoolConnection() throws SQLException {
        if(currentPoolSize == maxPoolSize) { return; }

        Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
        pool.offer(new PooledConnection(conn, this));
        currentPoolSize++;

        mLog.info("Created connection {0}, currentPoolSize={1}, maxPoolSize={2}");
                  
    }

    public Connection borrowConnection() throws InterruptedException, SQLException {

        if(currentPoolSize < maxPoolSize) { openAndPoolConnection(); }
        // Borrowing thread will be blocked till connection
        // becomes available in the queue
        return pool.take();
    }

    public void surrenderConnection(Connection conn) {
        if(!(conn instanceof PooledConnection)) { return; }
        pool.offer(conn); // offer() as we do not want to go beyond capacity
    }
    
    public void forcefullyCloseConnection(Connection conn) {
    	
    	if(!(conn instanceof PooledConnection)) { return; }
    	
        currentPoolSize--;
        if (null != conn) {
            try {
				conn.close();
			} catch (SQLException e) {
				// Ignore it is being removed from pool
			}
        }
        mLog.warn("Connection forcefully closed. Couner size "+ currentPoolSize +" Queue size "+pool.size() );
    }
}