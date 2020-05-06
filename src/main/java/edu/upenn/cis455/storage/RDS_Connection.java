package edu.upenn.cis455.storage;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class RDS_Connection {
    
    public static String connection_string;
    public static String port;
    public static String dbname;
    public static String username;
    public static String password;
    
    public RDS_Connection(String db_endpoint, String port, String dbname, String username, String password)
    {
        
//      conn = DriverManager.getConnection(  
//              "jdbc:mysql://testingdb.cu7l2h9ybbex.us-east-1.rds.amazonaws.com:3306/CIS455","admin","cis455crawler");
        
        RDS_Connection.port = port;
        RDS_Connection.dbname = dbname;
        RDS_Connection.username = username;
        RDS_Connection.password = password;
        connection_string = "jdbc:mysql://" + db_endpoint + ":" + port + "/" + dbname;
        //System.out.println(connection_string);
    
    }
        
    //Function for encoding passwords
    public static String encodeHex(byte[] digest) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < digest.length; i++) {
            sb.append(Integer.toString((digest[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

    //Function to set up hash
    public static String digest(String alg, String input) {
        try {
            MessageDigest md = MessageDigest.getInstance(alg);
            byte[] buffer = input.getBytes("UTF-8");
            md.update(buffer);
            byte[] digest = md.digest();
            return encodeHex(digest);
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }
    
    //Function to write hostnames and allowed paths to ALLOWS table
    public void allowed_write(String hostname, List<String> allowed_paths) {
        
        try{
        Class.forName("com.mysql.cj.jdbc.Driver");  
        Connection conn = DriverManager.getConnection(connection_string, username , password);
                
         Statement st = conn.createStatement(); 
         for (String path: allowed_paths) {
             st.executeUpdate("INSERT INTO ALLOWS (ALLOWED, HOSTNAME) " + 
                     "VALUES ('"+ path +"', '" + hostname +"')"); 
         }
         conn.close(); 

        }
        catch (SQLException e) {
            // TODO Auto-generated catch block
//            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
//            e.printStackTrace();
        }
        
    }
    //Function to write hostnames and disallowed paths to DISALLOWS table

    public void disallowed_write(String hostname, List<String> disallowed_paths) {
//        System.out.println(disallowed_paths + " " + disallowed_paths.size());
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");  
            Connection conn = DriverManager.getConnection(connection_string, username, password);
                    
             Statement st = conn.createStatement(); 
             for (String path: disallowed_paths) {
                 st.executeUpdate("INSERT INTO DISALLOWS (DISALLOWED, HOSTNAME) " + 
                         "VALUES ('"+ path +"', '" + hostname +"')"); 
             }
             conn.close(); 

            }
            catch (SQLException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            }
    
    }
    
    //Function to write hostnames and associtaed delay to CRAWLDELAY table
    public synchronized void crawldelay_write(String hostname, int delay) {
        
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");  
            Connection conn = DriverManager.getConnection(connection_string, username , password);
                    
             Statement st = conn.createStatement(); 
             st.executeUpdate("INSERT INTO CRAWLDELAY " + 
                         "VALUES ('"+ hostname +"', " + String.valueOf(delay) +")"); 
             
             conn.close(); 

            }
            catch (SQLException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            }
        
    }
    
    //Function to write hostnames and crawl timestamp to URLCRAWLTIME
    public void crawltime_write(String hostname, long timestamp) {
        
        String hashed_hostname = digest("SHA-256", hostname);
        
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");  
            Connection conn = DriverManager.getConnection(connection_string, username , password);
                    
             Statement st = conn.createStatement(); 
             st.executeUpdate("INSERT INTO URLCRAWLTIME " + 
                         "VALUES ('"+ hashed_hostname +"', '" + hostname +"', " + String.valueOf(timestamp) +")"); 
             
             conn.close(); 

            }
            catch (SQLException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            }
        
    
    }
    
    //Function to check if a particular hostname, filepath is allowed
    public boolean check_allow(String hostname, String filepath) {
        
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");  
            Connection conn = DriverManager.getConnection(connection_string, username , password);
                    
             Statement st = conn.createStatement(); 
             String query = "SELECT ALLOWED FROM ALLOWS WHERE HOSTNAME='" + hostname +"' " + "AND ALLOWED='" + filepath + "'";
             ResultSet rs = st.executeQuery(query);
             
             if (rs.next() == false) {
                    conn.close();
                    return false;
                  }
             else
             {
                 conn.close();
                 return true;
             }
            }
            catch (SQLException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            }
        return false;       
    }
    
    //function to check if hostname path combination is disallowed
    public boolean check_disallow(String hostname, String filepath) { 
        
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");  
            Connection conn = DriverManager.getConnection(connection_string, username , password);
                    
             Statement st = conn.createStatement(); 
             String query = "SELECT DISALLOWED FROM DISALLOWS WHERE HOSTNAME='" + hostname + "' " + "AND DISALLOWED='" + filepath + "'";
             ResultSet rs = st.executeQuery(query);
             
             if (rs.next() == false) {
                    conn.close();
                    return false;
                  }
             else
             {
                 conn.close();
                 return true;
             }
            }
            catch (SQLException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            }
            
        return false;}
    
    //Function to return crawldelay for a particular hostname
    public synchronized int get_crawldelay(String hostname) {
        
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");  
            Connection conn = DriverManager.getConnection(connection_string, username , password);
                    
             Statement st = conn.createStatement(); 
             String query = "SELECT DELAY FROM CRAWLDELAY WHERE HOSTNAME='" + hostname + "'";
             ResultSet rs = st.executeQuery(query);
             int delay = 0;
             while (rs.next()) {
                    delay = rs.getInt("DELAY");
                }
             conn.close();
             return delay;
        }
        catch (SQLException e) {
            // TODO Auto-generated catch block
//            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
//            e.printStackTrace();
        }
        
        return 0;
        
    }
    
    //function to get particuar crawltime based on hostname
    public long get_crawltime(String hostname) {

        try{
            String hashed_hostname = digest("SHA-256", hostname);
            Class.forName("com.mysql.cj.jdbc.Driver");  
            Connection conn = DriverManager.getConnection(connection_string, username , password);
                    
             Statement st = conn.createStatement(); 
             String query = "SELECT CRAWLTIME FROM URLCRAWLTIME WHERE URLHASH='" + hashed_hostname +"'";
             ResultSet rs = st.executeQuery(query);
             long time = 0;
             while (rs.next()) {
                    time = rs.getLong("CRAWLTIME");
                }
             conn.close();
             return time;
        }
        catch (SQLException e) {
            // TODO Auto-generated catch block
//            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
//            e.printStackTrace();
        }
        
        return 0;
        
    }

    //Execute any update table query
    public void executeUpdate(String query)
    {
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");  
            Connection conn = DriverManager.getConnection(connection_string, username , password);
                    
             Statement st = conn.createStatement(); 
             st.executeUpdate(query); 
             conn.close(); 

            }
            catch (SQLException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
//                e.printStackTrace();
            }
        
    }
    
    //execute any select query
    public ResultSet executeQuery(String query)
    {
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");  
            Connection conn = DriverManager.getConnection(connection_string, username , password);
                    
             Statement st = conn.createStatement(); 
             ResultSet rs = st.executeQuery(query);
             conn.close();
             return rs;
        }
        catch (SQLException e) {
            // TODO Auto-generated catch block
//            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
//            e.printStackTrace();
        }
        
        return null;
        
    }
    
    //testing
    
    public static void main(String[] args) {
        
        //creating connection object with endpoint, portnumber, dbname, username, password
        RDS_Connection rds = new RDS_Connection("testingdb.cu7l2h9ybbex.us-east-1.rds.amazonaws.com", "3306", "CIS455", "admin", "cis455crawler");
        
//      ArrayList<String> temp_list = new ArrayList<String>();
//      temp_list.add("/hello");
//      temp_list.add("/from");
//      temp_list.add("/the");
//      temp_list.add("/other");
//      temp_list.add("/side");
//      
//      rds.allowed_write("foo.com", temp_list);
//      rds.disallowed_write("foo.com", temp_list);
//      
//      System.out.println(rds.check_allow("foo.com", "/hello"));
//      System.out.println(rds.check_disallow("foo.com", "/hello"));
//      
//      System.out.println(rds.check_allow("foo.com", "/music"));
//      System.out.println(rds.check_disallow("foo.com", "/music"));
        
//      rds.crawldelay_write("google.com", 5);
//      rds.crawldelay_write("yahoo.com", 20);
//      
//      System.out.println(rds.get_crawldelay("google.com"));
//      System.out.println(rds.get_crawldelay("yahoo.com"));
//      System.out.println(rds.get_crawldelay("foo.com"));
        
//      long now = Instant.now().getEpochSecond();
        
//      rds.crawltime_write("google.com", now);
//      rds.crawltime_write("yahoo.com", now);
//      System.out.println(rds.get_crawltime("google.com"));
//      System.out.println(rds.get_crawltime("yahoo.com"));
//      System.out.println(rds.get_crawldelay("foo.com"));
        
        
    }
}
