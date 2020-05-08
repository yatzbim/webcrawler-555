package edu.upenn.cis455.crawler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HttpsURLConnection;

import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.xml.sax.SAXException;
import org.apache.tika.parser.Parser;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.RDS_Connection;

public class CrawlerBolt implements IRichBolt {
    static Logger log = Logger.getLogger(CrawlerBolt.class);
    String executorId = UUID.randomUUID().toString();

    static XPathCrawler instance = XPathCrawler.getInstance();

    Fields schema = new Fields("url");

    OutputCollector collector;

    static AtomicInteger idle = new AtomicInteger(0);

    public CrawlerBolt() {
    }

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
        instance.s.close();
    }

    public static boolean isIdle() {
        return idle.get() == 0;
    }

    @Override
    public void execute(Tuple input) {
        if (instance.shouldQuit()) {
            idle.set(0);
            return;
        }
        
        idle.incrementAndGet();

        instance.inFlight.decrementAndGet();

        String curr = input.getStringByField("url");

        if (instance.udpWorks) {
            byte[] data = ("jonahmil;" + curr).getBytes();
            DatagramPacket packet = new DatagramPacket(data, data.length, instance.host, 10455);
            try {
                instance.s.send(packet);
            } catch (IOException e) {
                System.err.println("Error sending UDP monitoring packet for " + curr + ". Continuing");
            }
        }

        // for HTTP
        HttpURLConnection httpConn = null;
        HttpURLConnection.setFollowRedirects(false);
        URL url;

        // for HTTPS
        HttpsURLConnection httpsConn = null;
        HttpsURLConnection.setFollowRedirects(false);

        URLInfo uInfo = new URLInfo(curr);

        String hostPort = uInfo.getHostName() + ":" + uInfo.getPortNo();

        if (instance.downloads.get() >= XPathCrawler.maxFiles && XPathCrawler.maxFiles > 0) {
            System.out.println("Max Files Achieved. Exiting crawl");
            instance.shutdown();
            idle.decrementAndGet();
            return;
        }

        try {
            url = new URL(curr);
        } catch (MalformedURLException e) {
            System.err.println("Bad HTTPS URL. Continuing");
            idle.decrementAndGet();
            return;
        }

        String robotsTxtSite = hostPort + "/robots.txt";

        if (curr.startsWith("http://")) {
            robotsTxtSite = "http://" + robotsTxtSite;
            // crawl http link

            try {
                httpConn = (HttpURLConnection) url.openConnection();
            } catch (IOException e) {
                System.err.println("Problem connecting to HTTPS URL. Continuing");
                idle.decrementAndGet();
                return;
            }

            // only enter this conditional if a robots.txt exists
            
            boolean delayAllows = true;
//            synchronized (XPathCrawler.accessLock) {
                if (instance.lastAccessed.containsKey(hostPort) && instance.lastAccessed.get(hostPort) > new Date().getTime()) {
//                    System.out.println("Delaying (http)");
                    delayAllows = false;
                }
//            }

            if (!delayAllows) {
                instance.frontier.add(curr);
                httpConn.disconnect();
                idle.decrementAndGet();
                return;
            }
            
            int delay = XPathCrawler.rds.get_crawldelay(hostPort);
            if (delay == -1) {
                delay = 0;
            }

            // since we've waited long enough, update the last access
            synchronized (XPathCrawler.accessLock) {
//                System.out.println("New Access: " + hostPort);
                instance.lastAccessed.put(hostPort, new Date().getTime() + (delay * 1000));
            }

            // check if it's allowed
            boolean isAllowed = XPathCrawler.rds.check_allow(hostPort, uInfo.getFilePath());
            boolean isDisallowed = XPathCrawler.rds.check_disallow(hostPort, uInfo.getFilePath());
            if (isDisallowed && !isAllowed) {
                System.out.println("Not permitted to crawl " + curr + ". Continuing");
                XPathCrawler.rds.crawltime_write(curr, new Date().getTime());
                httpConn.disconnect();
                idle.decrementAndGet();
                return;
            }

            // send HEAD request
            try {
                httpConn.setRequestMethod("HEAD");
            } catch (ProtocolException e1) {
                System.err.println("Error setting HEAD request method for " + curr);
                httpConn.disconnect();
                idle.decrementAndGet();
                return;
            }
            httpConn.setRequestProperty("Host", hostPort);
            httpConn.setRequestProperty("User-Agent", XPathCrawler.USER_AGENT);
            httpConn.setRequestProperty("Accept", "text/html");
            httpConn.setRequestProperty("Accept-Language", "en");

            // TODO: add language filtering (boolean langKnown, request header
            // accept-language)
            instance.incrHeadsSent();

            int hCode = 0;
            try {
                hCode = httpConn.getResponseCode();
            } catch (IOException e) {
                System.err.println("Error getting HEAD response code for " + curr);
            }

            if (hCode >= 300 && hCode < 400) {
                // handle redirect (3xx) response code
                String location = httpConn.getHeaderField("Location");
                if (XPathCrawler.rds.get_crawltime(curr) > 0) {
                    httpConn.disconnect();
                    idle.decrementAndGet();
                    return;
                }
                
                XPathCrawler.rds.crawltime_write(curr, new Date().getTime());
                if (curr.equals(location)) {
                    httpConn.disconnect();
                    idle.decrementAndGet();
                    return;
                }
                
                instance.frontier.add(location);
                System.out.println("Redirection of type " + hCode + " found from " + curr + " to " + location);
                httpConn.disconnect();
                idle.decrementAndGet();
                return;
            } else if (hCode >= 400) {
                // handle error (4xx, 5xx) response code
                XPathCrawler.rds.crawltime_write(curr, new Date().getTime());
                System.err.print(curr + ": " + hCode + " ");
                switch (hCode) {
                case 400:
                    System.err.print("Bad request");
                    break;
                case 403:
                    System.err.print("Forbidden");
                    break;
                case 404:
                    System.err.print("Content not found");
                    break;
                case 405:
                    System.err.print("Not Allowed");
                    break;
                case 406:
                    System.err.print("File type received was not HTML or was not English");
                    break;
                case 409:
                    System.err.print("CETS error. Check the format of your request");
                default:
                    System.err.print(": Developer needs to get off his ass and do some debugging");
                }
                System.err.println(" - Continuing");
                httpConn.disconnect();
                idle.decrementAndGet();
                return;
            }

            boolean downloadable = true;

            // determine content length
            // TODO: figure out a good max size
            int contentLength = httpConn.getContentLength();
            if (contentLength > XPathCrawler.maxSize) {
                downloadable = false;
            }

            // determine content type

            // TODO: check on character encoding. Do we care? (Probably)
            String contentType = httpConn.getContentType();
            if (contentType != null) {
                contentType = contentType.split(";")[0];
            }

            downloadable = downloadable && contentType != null && contentType.endsWith("/html");

            if (!downloadable) {
                System.out.println(curr + " is not an HTML doc - not downloading or crawling");
                httpConn.disconnect();
                idle.decrementAndGet();
                return;
            }

            // determine whether document has already been crawled
//            long lastMod = httpConn.getLastModified();

            // determine whether doc has already been downloaded
            
            // TODO: comments below
//            long lastCrawl = XPathCrawler.rds.get_crawltime(curr);
//            if (lastCrawl > 0) {
//                // determine whether doc needs to be downloaded again
////                if (lastMod <= lastCrawl) {
////                    System.err.println("Not downloading " + curr + " - we already have the most up-to-date version");
////                    downloadable = false;
////                }
//                System.err.println("Not downloading " + curr + " - it's already been crawled");
//                downloadable = false;
//                httpConn.disconnect();
//                idle.decrementAndGet();
//                return;
//            }
            // TODO: comments above
            httpConn.disconnect();
            instance.inFlight.incrementAndGet();

            collector.emit(new Values<Object>(curr));
            idle.decrementAndGet();
        } else if (curr.startsWith("https://")) {
            // case to crawl HTTPS links
            robotsTxtSite = "https://" + robotsTxtSite;

            // establish connection to URL
            try {
                httpsConn = (HttpsURLConnection) url.openConnection();
            } catch (IOException e) {
                System.err.println("Problem connecting to HTTPS URL. Continuing");
                idle.decrementAndGet();
                return;
            }

            boolean delayAllows = true;
//            synchronized (XPathCrawler.accessLock) {
                if (instance.lastAccessed.get(hostPort) != null && instance.lastAccessed.get(hostPort) > new Date().getTime()) {
//                    System.out.println("Delaying (https)");
                    delayAllows = false;
                }
//            }

            if (!delayAllows) {
                instance.frontier.add(curr);
                httpsConn.disconnect();
                idle.decrementAndGet();
                return;
            }

            // check if it's allowed
            boolean isAllowed = XPathCrawler.rds.check_allow(hostPort, uInfo.getFilePath());
            boolean isDisallowed = XPathCrawler.rds.check_disallow(hostPort, uInfo.getFilePath());
            if (isDisallowed && !isAllowed) {
                System.out.println("Not permitted to crawl " + curr + ". Continuing");
                XPathCrawler.rds.crawltime_write(curr, new Date().getTime());
                httpsConn.disconnect();
                idle.decrementAndGet();
                return;
            }

            int delay = XPathCrawler.rds.get_crawldelay(hostPort);
            if (delay == -1) {
                delay = 0;
            }
            // since we've waited long enough, update the last access
            synchronized (XPathCrawler.accessLock) {
//                System.out.println("New Access: " + hostPort);
                instance.lastAccessed.put(hostPort, new Date().getTime() + (delay * 1000));
            }

            // send HEAD request
            try {
                httpsConn.setRequestMethod("HEAD");
            } catch (ProtocolException e1) {
                System.err.println("Error setting HEAD request method for " + curr);
                httpsConn.disconnect();
                idle.decrementAndGet();
                return;
            }
            httpsConn.setRequestProperty("Host", hostPort);
            httpsConn.setRequestProperty("User-Agent", XPathCrawler.USER_AGENT);
            httpsConn.setRequestProperty("Accept", "text/html");
            httpsConn.setRequestProperty("Accept-Language", "en");

            instance.incrHeadsSent();

            int hCode = 0;
            try {
                hCode = httpsConn.getResponseCode();
            } catch (IOException e) {
                System.err.println("Error getting HEAD response code for " + curr);
            }

            if (hCode >= 300 && hCode < 400) {
                // handle redirect (3xx) response code
                String location = httpsConn.getHeaderField("Location");
                
                if (XPathCrawler.rds.get_crawltime(curr) > 0) {
                    httpsConn.disconnect();
                    idle.decrementAndGet();
                    return;
                }
                
                XPathCrawler.rds.crawltime_write(curr, new Date().getTime());
                if (curr.equals(location)) {
                    httpsConn.disconnect();
                    idle.decrementAndGet();
                    return;
                }
                
                instance.frontier.add(location);
                System.out.println("Redirection of type " + hCode + " found from " + curr + " to " + location);

                httpsConn.disconnect();
                idle.decrementAndGet();
                return;
            } else if (hCode >= 400) {
                XPathCrawler.rds.crawltime_write(curr, new Date().getTime());
                // handle error (4xx, 5xx) response code
                System.err.print(curr + ": " + hCode + " ");
                switch (hCode) {
                case 400:
                    System.err.print("Bad request");
                    break;
                case 403:
                    System.err.print("Forbidden");
                    break;
                case 404:
                    System.err.print("Content not found");
                    break;
                case 405:
                    System.err.print("Not Allowed");
                    break;
                case 406:
                    System.err.print("File type received was not HTML or was not English");
                    break;
                case 409:
                    System.err.print("CETS error. Check the format of your request");
                default:
                    System.err.print(": Developer needs to get off his ass and do some debugging");
                }
                System.err.println(" - Continuing");
                httpsConn.disconnect();
                idle.decrementAndGet();
                return;
            }

            boolean downloadable = true;

            // determine content length
            // TODO: come up with a good maxsize
            int contentLength = httpsConn.getContentLength();
            if (contentLength > XPathCrawler.maxSize) {
                downloadable = false;
            }

            // determine content type
            String contentType = null;
            if (httpsConn.getContentType() != null) {
                contentType = httpsConn.getContentType().split(";")[0];
            }
            
            downloadable = downloadable && contentType != null && contentType.endsWith("/html");

            if (!downloadable) {
                System.out.println(curr + " is not an HTML doc - not downloading or crawling");
                idle.decrementAndGet();
                return;
            }

            // determine whether document has already been crawled
//            long lastMod = httpsConn.getLastModified();

            // determine whether doc has already been downloaded
            
            // TODO: below comments
//            long lastCrawl = XPathCrawler.rds.get_crawltime(curr);
//            if (lastCrawl > 0) {
//                // determine whether doc needs to be downloaded again
////                if (lastMod <= lastCrawl) {
////                    System.err.println("Not downloading " + curr + " - we already have the most up-to-date version");
////                    downloadable = false;
////                }
//                System.err.println("Not downloading " + curr + " - it's already been crawled");
//                downloadable = false;
//                httpsConn.disconnect();
//                idle.decrementAndGet();
//                return;
//            }
            // TODO: above comments
            
            httpsConn.disconnect();
            instance.inFlight.incrementAndGet();
            collector.emit(new Values<Object>(curr));
            idle.decrementAndGet();
        } else {
            idle.decrementAndGet();
            return;
        }
    }

    @Override
    public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void setRouter(IStreamRouter router) {
        this.collector.setRouter(router);
    }

    @Override
    public Fields getSchema() {
        return this.schema;
    }
    
    
    public static void main(String[] args) {
        RDS_Connection rds = new RDS_Connection("testingdb.cu7l2h9ybbex.us-east-1.rds.amazonaws.com", "3306", "CIS455_newdb", "admin", "cis455crawler");
//        String s = "https://en.wikipedia.org/wiki/Main_Page";
//        System.out.println(RDS_Connection.digest("SHA-256", s));
        System.out.println(rds.check_disallow("www.reddit.com:443", "/filler.embed"));
    }
}
