package edu.upenn.cis455.crawler;

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

public class CrawlerBolt implements IRichBolt {
    static Logger log = Logger.getLogger(CrawlerBolt.class);
    String executorId = UUID.randomUUID().toString();

    static XPathCrawler instance = XPathCrawler.getInstance();

    Fields schema = new Fields("url", "downloadable", "crawlable");

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
            int delay = XPathCrawler.rds.get_crawldelay(hostPort);
            if (delay > 0) { // TODO: should this be more specific? There could be a robots.txt with no delay
                boolean delayAllows = true;
                long now = new Date().getTime();
                if (instance.lastAccessed.get(hostPort) != null && instance.lastAccessed.get(hostPort) > now) {
                    delayAllows = false;
                }

                if (!delayAllows) {
                    instance.frontier.add(curr);
                    httpConn.disconnect();
                    idle.decrementAndGet();
                    return;
                }
                
                // check if it's allowed
                boolean isAllowed = XPathCrawler.rds.check_allow(hostPort, uInfo.getFilePath());
                boolean isDisallowed = XPathCrawler.rds.check_disallow(hostPort, uInfo.getFilePath());
                if (isDisallowed && !isAllowed) {
                    System.out.println("Not permitted to crawl " + curr + ". Continuing");
                    httpConn.disconnect();
                    idle.decrementAndGet();
                    return;
                }

                // since we've waited long enough, update the last access
                instance.lastAccessed.put(hostPort, new Date().getTime() + (delay * 1000));
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

            // TODO: add language filtering (boolean langKnown, request header accept-language)
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
                instance.frontier.add(location);
                System.out.println("Redirection of type " + hCode + " found from " + curr + " to " + location);
                httpConn.disconnect();
                idle.decrementAndGet();
                return;
            } else if (hCode >= 400) {
                // handle error (4xx, 5xx) response code
                System.err.print(curr + ": ");
                switch (hCode) {
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
                httpConn.disconnect();
                idle.decrementAndGet();
                return;
            }

            // TODO: change downloads to html only
            boolean downloadable = true;

            // determine content length
            // TODO: figure out a good max size
            int contentLength = httpConn.getContentLength();
            if (contentLength > XPathCrawler.maxSize) {
                downloadable = false;
            }

            // determine content type

            // TODO: check on character encoding. Do we care? (Probably)
            String contentType = httpConn.getContentType().split(";")[0];
            
            downloadable = contentType.endsWith("/html");
            
            boolean crawlable = contentType.endsWith("/html");
            
            if (!downloadable) {
                System.out.println(curr + " is not an HTML doc - not downloading or crawling");
            }

            // determine whether document has already been crawled
            long lastMod = httpConn.getLastModified();

            // determine whether doc has already been downloaded
            long lastCrawl = XPathCrawler.rds.get_crawltime(curr);
            if (lastCrawl > 0) {
                // determine whether doc needs to be downloaded again
                if (lastMod <= lastCrawl) {
                    System.err.println("Not downloading " + curr + " - we already have the most up-to-date version");
                    downloadable = false;
                }
            }
            httpConn.disconnect();
            instance.inFlight.incrementAndGet();

            collector.emit(new Values<Object>(curr, downloadable, crawlable));
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

            int delay = XPathCrawler.rds.get_crawldelay(hostPort);
            if (delay > 0) { // TODO: should this be more specific? There could be a robots.txt with no delay
                boolean delayAllows = true;
                long now = new Date().getTime();
                if (instance.lastAccessed.get(hostPort) != null && instance.lastAccessed.get(hostPort) > now) {
                    delayAllows = false;
                }

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
                    httpsConn.disconnect();
                    idle.decrementAndGet();
                    return;
                }

                // since we've waited long enough, update the last access
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

            instance.incrHeadsSent();

            // TODO: add language support (only accept english, and boolean if language is known)

            int hCode = 0;
            try {
                hCode = httpsConn.getResponseCode();
            } catch (IOException e) {
                System.err.println("Error getting HEAD response code for " + curr);
            }

            if (hCode >= 300 && hCode < 400) {
                // handle redirect (3xx) response code
                String location = httpsConn.getHeaderField("Location");
                instance.frontier.add(location);
                System.out.println("Redirection of type " + hCode + " found from " + curr + " to " + location);
                httpsConn.disconnect();
                idle.decrementAndGet();
                return;
            } else if (hCode >= 400) {
                // handle error (4xx, 5xx) response code
                System.err.print(curr + ": ");
                switch (hCode) {
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

            String contentType = httpsConn.getContentType().split(";")[0];
            
            downloadable = contentType.endsWith("/html");
            
            boolean crawlable = contentType.endsWith("/html");
            
            if (!downloadable) {
                System.out.println(curr + " is not an HTML doc - not downloading or crawling");
            }

            // determine whether document has already been crawled
            long lastMod = httpsConn.getLastModified();

            // determine whether doc has already been downloaded
            long lastCrawl = XPathCrawler.rds.get_crawltime(curr);
            if (lastCrawl > 0) {
                // determine whether doc needs to be downloaded again
                if (lastMod <= lastCrawl) {
                    System.err.println("Not downloading " + curr + " - we already have the most up-to-date version");
                    downloadable = false;
                }
            }
            httpsConn.disconnect();
            instance.inFlight.incrementAndGet();
            collector.emit(new Values<Object>(curr, downloadable, crawlable));
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

    // Forms HTTP request to GET robots.txt
    // used in HTTP
    public static String getRobotsTxt(String url, String hostname) {
        StringBuilder sb = new StringBuilder();

        sb.append("GET " + url + " HTTP/1.1\r\n");
        sb.append("Host: " + hostname + "\r\n");
        sb.append("User-Agent:" + XPathCrawler.USER_AGENT +"\r\n");
        sb.append("Accept: text/plain\r\n\r\n");

        return sb.toString();
    }

    // Forms HTTP request for HEAD based on host and URL
    // used in HTTP
    public static String headRequest(String url, String hostname) {
        StringBuilder sb = new StringBuilder();

        sb.append("HEAD " + url + " HTTP/1.1\r\n");
        sb.append("Host: " + hostname + "\r\n");
        sb.append("User-Agent: " + XPathCrawler.USER_AGENT +"\r\n");
        sb.append("Accept: text/html, text/xml, application/xml, */*+xml\r\n\r\n");

        return sb.toString();
    }

    // Determines the redirect location for a link
    // used in HTTP
    public static String getLocation(String headerString) {
        String[] headers = headerString.split("\r\n");
        for (String header : headers) {
            String[] keyVal = header.split(": ?[^/a-zA-z]");
            if (keyVal.length != 2) {
                continue;
            }
            String key = keyVal[0].trim();
            String val = keyVal[1].trim();

            if (key.equalsIgnoreCase("Location")) {
                return val;
            }
        }

        return null;
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
        conn.setRequestProperty("User-Agent", XPathCrawler.USER_AGENT);
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
        conn.setRequestProperty("User-Agent", XPathCrawler.USER_AGENT);
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
