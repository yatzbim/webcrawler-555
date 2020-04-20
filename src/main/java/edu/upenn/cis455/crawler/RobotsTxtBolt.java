package edu.upenn.cis455.crawler;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HttpsURLConnection;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.URLInfo;

public class RobotsTxtBolt implements IRichBolt {
    static Logger log = Logger.getLogger(RobotsTxtBolt.class);
    String executorId = UUID.randomUUID().toString();
    
    static XPathCrawler instance = XPathCrawler.getInstance();
    
    Fields schema = new Fields("url");
    OutputCollector collector;
    
    static AtomicInteger idle = new AtomicInteger(0);
    
    @Override
    public String getExecutorId() {
        return this.executorId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
        // Do nothing (?)
    }
    
    public static boolean isIdle() {
        return idle.get() == 0;
    }

    @Override
    public void execute(Tuple input) {
        idle.incrementAndGet();
        
        instance.inFlight.decrementAndGet();
        
        String curr = input.getStringByField("url").trim();

        String currRobotsTxt = null;

        URLInfo uInfo = new URLInfo(curr);
        String robotsTxtSite = uInfo.getHostName() + ":" + uInfo.getPortNo() + "/robots.txt";       
        
        if (curr.startsWith("http://")) {
            robotsTxtSite = "http://" + robotsTxtSite;
            // crawl http link

            // retrieve robots.txt
            if (instance.db.getRobotsTxt(robotsTxtSite) != null) {
                // if we've seen the robots.txt before
                currRobotsTxt = instance.db.getRobotsTxt(robotsTxtSite);
            } else {
                // if we've never seen the robots.txt before
                try {
                    currRobotsTxt = getHttpRobotsTxt(uInfo.getHostName() + ":" + uInfo.getPortNo());
                } catch (UnknownHostException e1) {
                    System.err.println("Failed to connect to http://" + uInfo.getHostName() + ":" + uInfo.getPortNo() + "/robots.txt");
                    idle.decrementAndGet();
                    return;
                }
                if (currRobotsTxt == null) {
                    System.out.println("Null robots.txt for " + curr + ". Continuing");
                    idle.decrementAndGet();
                    return;
                }

                try {
                    instance.db.addRobotsTxt(robotsTxtSite, currRobotsTxt);
                } catch (Exception e) {
                    System.err.println("Issue saving robots.txt for " + uInfo.getHostName() + ":" + uInfo.getPortNo() + " - Continuing");
                }
            }
        } else if (curr.startsWith("https://")) {
         // case to crawl HTTPS links
            robotsTxtSite = "https://" + robotsTxtSite;
            
            // retrieve robots.txt
            if (instance.db.getRobotsTxt(robotsTxtSite) != null) {
                // if we've seen the robots.txt before
                currRobotsTxt = instance.db.getRobotsTxt(robotsTxtSite);
            } else {
                // if we've never seen the robots.txt before
                try {
                    currRobotsTxt = getHttpsRobotsTxt(uInfo.getHostName() + ":" + uInfo.getPortNo());
                } catch (UnknownHostException e1) {
                    System.err.println("Failed to connect to https://" + uInfo.getHostName() + ":" + uInfo.getPortNo() + "/robots.txt");
                    idle.decrementAndGet();
                    return;
                }
                if (currRobotsTxt == null) {
                    System.out.println("Null robots.txt for " + curr + ". Continuing");
                    idle.decrementAndGet();
                    return;
                }

                try {
                    instance.db.addRobotsTxt(robotsTxtSite, currRobotsTxt);
                } catch (Exception e) {
                    System.err.println("Issue saving robots.txt for " + uInfo.getHostName() + ":" + uInfo.getPortNo() + " - Continuing");
                }
            }
        } else {
            idle.decrementAndGet();
            return;
        }
        
        // TODO: refit with AWS DBs
        
        collector.emit(new Values<Object>(curr));
        
    }

    @Override
    public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void setRouter(IStreamRouter router) {
        collector.setRouter(router);
    }

    @Override
    public Fields getSchema() {
        return this.schema;
    }

 // Get robots.txt for a new HTTPS host
    public static String getHttpsRobotsTxt(String robotsTxtHost) throws UnknownHostException {
        URL url = null;
        try {
            url = new URL("https://" + robotsTxtHost + "/robots.txt");
        } catch (MalformedURLException e2) {
            System.err.println("Bad URL: https://" + robotsTxtHost + "/robots.txt");
            return null;
        }

        HttpsURLConnection conn = null;
        try {
            conn = (HttpsURLConnection) url.openConnection();
        } catch (IOException e2) {
            System.err.println("Failed to connect to https://" + robotsTxtHost + "/robots.txt");
            return null;
        }

        try {
            conn.setRequestMethod("GET");
        } catch (ProtocolException e1) {
            System.err.println("Error setting robots.txt request method for " + robotsTxtHost);
            conn.disconnect();
            return null;
        }
        conn.setRequestProperty("Host", robotsTxtHost);
        conn.setRequestProperty("User-Agent", "cis455crawler");
        conn.setRequestProperty("Accept", "text/plain");

        int code;
        try {
            code = conn.getResponseCode();
        } catch (UnknownHostException e) {
            throw (e);
        } catch (IOException e) {
            System.err.println(e.toString());
            System.err.println("Error getting robots.txt response code for " + robotsTxtHost);
            conn.disconnect();
            return null;
        }
        if (code >= 400) {
            System.err.print("https://" + robotsTxtHost + "/robots.txt: ");
            switch (code) {
            case 400:
                System.err.print("Bad request");
                break;
            case 404:
                System.err.print("Content not found");
                break;
            case 406:
                System.err.print("File type received was not HTML, XML or RSS");
                break;
            case 409:
                System.err.print("CETS error. Check the format of your request");
            default:
                System.err.print("Other error. Developer needs to get off his ass and do some debugging");
            }
            System.err.println(" - Continuing");
            conn.disconnect();
            return null;
        }

        StringBuilder sb = new StringBuilder();

        InputStream input;
        try {
            input = conn.getInputStream();
        } catch (IOException e) {
            System.err.println("Error getting robots.txt input stream for " + robotsTxtHost);
            conn.disconnect();
            return null;
        }
        InputStreamReader in = new InputStreamReader(input);
        int i = 0;
        try {
            while (i != -1) {
                i = in.read();
                if (i != -1) {
                    sb.append((char) i);
                }

                if (!in.ready()) {
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Error getting robots.txt response for " + robotsTxtHost);
            conn.disconnect();
            try {
                input.close();
                in.close();
            } catch (IOException e1) {
            }
            return null;
        }

        String robotsResponse = sb.toString();

        if (robotsResponse.isEmpty()) {
            try {
                input.close();
                in.close();
            } catch (IOException e) {
                System.err.println("Error closing robots.txt input stream for " + robotsTxtHost);
            }
            conn.disconnect();
            return null;
        }

        conn.disconnect();

        return robotsResponse;
    }
    
    // Get robots.txt for a new HTTP host
    public static String getHttpRobotsTxt(String robotsTxtHost) throws UnknownHostException {
        URL url = null;
        try {
            url = new URL("http://" + robotsTxtHost + "/robots.txt");
        } catch (MalformedURLException e2) {
            System.err.println("Bad URL: http://" + robotsTxtHost + "/robots.txt");
            return null;
        }

        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
        } catch (IOException e2) {
            System.err.println("Failed to connect to http://" + robotsTxtHost + "/robots.txt");
            return null;
        }

        try {
            conn.setRequestMethod("GET");
        } catch (ProtocolException e1) {
            System.err.println("Error setting robots.txt request method for " + robotsTxtHost);
            conn.disconnect();
            return null;
        }
        conn.setRequestProperty("Host", robotsTxtHost);
        conn.setRequestProperty("User-Agent", "cis455crawler");
        conn.setRequestProperty("Accept", "text/plain");

        int code;
        try {
            code = conn.getResponseCode();
        } catch (UnknownHostException e) {
            throw (e);
        } catch (IOException e) {
            System.err.println(e.toString());
            System.err.println("Error getting robots.txt response code for " + robotsTxtHost);
            conn.disconnect();
            return null;
        }
        if (code >= 400) {
            System.err.print("http://" + robotsTxtHost + "/robots.txt: ");
            switch (code) {
            case 400:
                System.err.print("Bad request");
                break;
            case 404:
                System.err.print("Content not found");
                break;
            case 406:
                System.err.print("File type received was not HTML, XML or RSS");
                break;
            case 409:
                System.err.print("CETS error. Check the format of your request");
            default:
                System.err.print("Other error. Developer needs to get off his ass and do some debugging");
            }
            System.err.println(" - Continuing");
            conn.disconnect();
            return null;
        }

        StringBuilder sb = new StringBuilder();

        InputStream input;
        try {
            input = conn.getInputStream();
        } catch (IOException e) {
            System.err.println("Error getting robots.txt input stream for " + robotsTxtHost);
            conn.disconnect();
            return null;
        }
        InputStreamReader in = new InputStreamReader(input);
        int i = 0;
        try {
            while (i != -1) {
                i = in.read();
                if (i != -1) {
                    sb.append((char) i);
                }

                if (!in.ready()) {
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Error getting robots.txt response for " + robotsTxtHost);
            conn.disconnect();
            try {
                input.close();
                in.close();
            } catch (IOException e1) {
            }
            return null;
        }

        String robotsResponse = sb.toString();

        if (robotsResponse.isEmpty()) {
            try {
                input.close();
                in.close();
            } catch (IOException e) {
                System.err.println("Error closing robots.txt input stream for " + robotsTxtHost);
            }
            conn.disconnect();
            return null;
        }

        conn.disconnect();

        return robotsResponse;
    }
    
}
