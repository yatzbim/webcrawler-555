package edu.upenn.cis455.crawler;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HttpsURLConnection;

import org.apache.log4j.Logger;
import org.apache.tika.language.LanguageIdentifier;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.AWSDatabase;

@SuppressWarnings("deprecation")
public class DownloaderBolt implements IRichBolt {
    static Logger log = Logger.getLogger(CrawlerBolt.class);
    String executorId = UUID.randomUUID().toString();

    static XPathCrawler instance = XPathCrawler.getInstance();

    Fields schema = new Fields("links");

    OutputCollector collector;

    static AtomicInteger idle = new AtomicInteger(0);

    AWSDatabase aws = AWSDatabase.getInstance();

    public DownloaderBolt() {
        // log.debug("Starting downloader bolt");
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
    }

    public static boolean isIdle() {
        return idle.get() == 0;
    }

    @Override
    public synchronized void execute(Tuple input) {
        if (instance.shouldQuit()) {
            idle.set(0);
            return;
        }

        idle.incrementAndGet();

        instance.inFlight.decrementAndGet();

        // for HTTP
        HttpURLConnection httpConn = null;

        // for HTTPS
        URL url = null;
        HttpsURLConnection httpsConn = null;
        HttpsURLConnection.setFollowRedirects(false);

        String curr = input.getStringByField("url");
//        boolean downloadable = (boolean) input.getObjectByField("downloadable");

        URLInfo uInfo = new URLInfo(curr);

        List<String> newLinks = new LinkedList<>();

//        boolean downloadable = XPathCrawler.rds.get_crawltime(curr) == 0;
        
        if (XPathCrawler.rds.get_crawltime(curr) == 0) {
            XPathCrawler.rds.crawltime_write(curr, new Date().getTime());
            // jsoup
            Document doc = null;
            try {
                doc = Jsoup.connect(curr).userAgent("cis455crawler").get();
//                doc.getElementsByClass("header").remove();
//                doc.getElementsByClass("footer").remove();
                doc.charset(Charset.forName("UTF-8"));
            } catch (IOException e) {
                System.err.println("Error connecting to " + curr + " with JSoup. Continuing");
            }

            String text = null;
            LanguageIdentifier object = null;
            if (doc != null) {
                text = doc.text();
                object = new LanguageIdentifier(text);
            }

            if (object != null && !object.getLanguage().equals("en") && object.isReasonablyCertain()) {
                System.out.println(curr + " is not an english page.");
                idle.decrementAndGet();
                return;
            }
            // extract links
            if (doc != null) {
                System.out.println("Extracting links from " + curr);
                Elements linkElts = doc.select("a[href]");
                for (Element elt : linkElts) {
                    String rawLink = elt.attributes().get("href").trim();
                    if (rawLink.isEmpty() || rawLink.charAt(0) == '#'
                            || (rawLink.length() > 1 && rawLink.charAt(0) == '/' && rawLink.charAt(1) == '/')
                            || rawLink.startsWith("mailto:")) {
                        continue;
                    }
                    

                    String fullLink = constructLink(rawLink, curr, uInfo);
                    if (fullLink == null || fullLink.trim().equals(curr)) {
//                        System.out.println("continuing: " + fullLink);
                        continue;
                    }

                    newLinks.add(fullLink);
                }
                
//                System.out.println("here");
                aws.saveOutgoingLinks(curr, newLinks);

                // download document
                instance.downloads.incrementAndGet();
                System.out.println("Downloading " + curr);
                aws.savePage(curr, text);
            }
        }

        instance.inFlight.incrementAndGet();
        collector.emit(new Values<Object>(newLinks));
        idle.decrementAndGet();
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

    public static String constructLink(String href, String curr, URLInfo info) {
//        System.out.println(href);
        StringBuilder sb = new StringBuilder();
        
        if (href.contains("javascript:")) {
            return null;
        }
        
//        System.out.println("check 1");
        
        String[] removeHashtag = href.split("#");
        if (removeHashtag.length > 2) {
            return null;
        } else if (removeHashtag.length == 2 && removeHashtag[1].contains("/")) {
            return null;
        }
        
//        System.out.println("check 2");
        
        href = removeHashtag[0];

        if (!href.startsWith("http://") && !href.startsWith("https://")) {
            
            // TODO: build out to more unwanted links
            if ((info.getHostName().contains("wikipedia") && href.contains("index.php")) || href.contains("..")
                    || href.contains("twitter.com")) {
                return null;
            }
            
            // relative link
            if (href.charAt(0) == '/') {
                // start from host
                if (curr.startsWith("http://")) {
                    sb.append("http://");
                } else if (curr.startsWith("https://")) {
                    sb.append("https://");
                } else {
                    // we don't want it
                    return null;
                }

                sb.append(info.getHostName());
                if (info.getPortNo() != 80 && info.getPortNo() != 443) {
                    sb.append(":");
                    sb.append(info.getPortNo());
                }
                sb.append(href);
            } else {
                String noQuery = curr.split("\\?")[0];
                
                String[] linkPieces = noQuery.split("/");
                String bookend = linkPieces[linkPieces.length-1];
                if (!bookend.equals(info.getHostName()) && bookend.contains(".")) {
                    // last thing in path is a file, get rid of it before appending href
                    for (int i = 0; i < linkPieces.length - 1; i++) {
                        sb.append(linkPieces[i]);
                        sb.append('/');
                    }
                } else {
                    sb.append(noQuery);
                    if (noQuery.charAt(noQuery.length() - 1) != '/') {
                        sb.append('/');
                    }
                }

                sb.append(href);
            }

        } else {
            // absolute link
            sb.append(href);
//            System.out.println("check 3");
        }

        String fullLink = sb.toString();
        
        // only checks links which are 50 directories deep
        if (fullLink.split("/").length > 53) {
            return null;
        }
//        System.out.println("check 4: " + fullLink);
        return fullLink;
    }

    // get the document for an HTTP link
    public static String getDocumentHttps(HttpsURLConnection conn, String url) {
        try {
            conn.setRequestMethod("GET");
        } catch (ProtocolException e1) {
            System.err.println("Error setting GET request method for " + url);
            return null;
        }

        URLInfo info = new URLInfo(url);

        conn.setRequestProperty("Host", info.getHostName());
        conn.setRequestProperty("User-Agent", XPathCrawler.USER_AGENT);
        conn.setRequestProperty("Accept", "text/plain");

        int code;
        try {
            code = conn.getResponseCode();
        } catch (IOException e) {
            System.err.println("Error getting GET response code for " + url);
            return null;
        }
        if (code >= 400) {
            System.err.print(url + ": " + code + " ");
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
            return null;
        }

        StringBuilder sb = new StringBuilder();

        InputStream input;
        try {
            input = conn.getInputStream();
        } catch (IOException e) {
            System.err.println("Error getting GET input stream for " + url);
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
            System.err.println("Error getting GET response for " + url);
            return null;
        }

        String body = sb.toString();

        if (body.isEmpty()) {
            try {
                input.close();
            } catch (IOException e) {
                System.err.println("Error closing GET input stream for " + url);
            }
            return null;
        }

        return body;
    }

    // get the document for an HTTP link
    public static String getDocument(HttpURLConnection conn, String url) {
        try {
            conn.setRequestMethod("GET");
        } catch (ProtocolException e1) {
            System.err.println("Error setting GET request method for " + url);
            return null;
        }

        URLInfo info = new URLInfo(url);

        conn.setRequestProperty("Host", info.getHostName());
        conn.setRequestProperty("User-Agent", XPathCrawler.USER_AGENT);
        conn.setRequestProperty("Accept", "text/plain");

        int code;
        try {
            code = conn.getResponseCode();
        } catch (IOException e) {
            System.err.println("Error getting GET response code for " + url);
            return null;
        }
        if (code >= 400) {
            System.err.print(url + ": ");
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
            return null;
        }

        StringBuilder sb = new StringBuilder();

        InputStream input;
        try {
            input = conn.getInputStream();
        } catch (IOException e) {
            System.err.println("Error getting GET input stream for " + url);
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
            System.err.println("Error getting GET response for " + url);
            return null;
        }

        String body = sb.toString();

        if (body.isEmpty()) {
            try {
                input.close();
            } catch (IOException e) {
                System.err.println("Error closing GET input stream for " + url);
            }
            return null;
        }

        return body;
    }
    
    public static void main(String[] args) throws IOException {
        String link = "https://www.amazon.com";
        Document doc = Jsoup.connect(link)
                .userAgent("cis455crawler")
                .get();
//        System.out.println(doc.toString());
        
        Elements linkElts = doc.select("a[href]");
//        for (Element elt : linkElts) {
////            String rawLink = elt.attributes().get("href").trim();
////            String lang = elt.attributes().get("hreflang").trim();
//            System.out.println(elt);
//        }

    }
}
