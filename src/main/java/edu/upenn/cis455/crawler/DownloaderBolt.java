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
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.langdetect.TextLangDetector;
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
    
    final LanguageDetector detector = new OptimaizeLangDetector().loadModels();

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

        String curr = input.getStringByField("url");

        URLInfo uInfo = new URLInfo(curr);

        List<String> newLinks = new LinkedList<>();
        
        if (XPathCrawler.rds.get_crawltime(curr) == 0) {
            XPathCrawler.rds.crawltime_write(curr, new Date().getTime());
            // jsoup
            Document doc = null;
            try {
                doc = Jsoup.connect(curr).timeout(5000).userAgent("cis455crawler").get();
//                doc.getElementsByClass("header").remove();
//                doc.getElementsByClass("footer").remove();
                doc.charset(Charset.forName("UTF-8"));
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Error connecting to " + curr + " with JSoup. Continuing");
            }

            String text = null;
            LanguageResult langResult = null;
            if (doc != null) {
                text = doc.text();
                langResult = detector.detect(text);
            }

            if (langResult != null && ((!langResult.getLanguage().equals("en") && langResult.isReasonablyCertain())
                    || (langResult.getLanguage().equals("en") && !langResult.isReasonablyCertain()))) {
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
                    if (rawLink.isEmpty() || rawLink.charAt(0) == '#') {
                        continue;
                    }
                    
                    // TODO: ¯\_(ツ)_/¯
                    if ((rawLink.length() > 1 && rawLink.charAt(0) == '/' && rawLink.charAt(1) == '/')) {
                        rawLink = "https:" + rawLink;
                    }

                    String fullLink = constructLink(rawLink, curr, uInfo);
                    if (fullLink == null || fullLink.trim().equals(curr)) {
//                        System.out.println("continuing: " + rawLink);
                        continue;
                    }

                    if (fullLink.startsWith("http://www.imdb.com") || fullLink.startsWith("http://www.hulu.com")) {
                        fullLink = fullLink.replaceFirst("http:", "https:");
                    }
                    
                    newLinks.add(fullLink);
                }
                
                aws.saveOutgoingLinks(curr, newLinks);

                // download document
                System.out.println("Downloading " + curr);
                aws.savePage(curr, text);
                instance.downloads.incrementAndGet();
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
        StringBuilder sb = new StringBuilder();
        
        if (href.contains("..") || href.contains("'")) {
            return null;
        }
        
        href = href.replace("./", "");
        href = href.replace(" ", "%20");
        
        String[] removeHashtag = href.split("#");
        if (removeHashtag.length > 2) {
            return null;
        } else if (removeHashtag.length == 2 && removeHashtag[1].contains("/")) {
            return null;
        }
        
        href = removeHashtag[0];
        
        // TODO: build out to more unwanted links
//        System.out.println("HREF: " + href);
        if (href.contains("javascript:") || href.contains("mailto:") || href.equals(".") || href.equals("'")
                || href.endsWith("/robots.txt") || href.contains("twitter.com") || href.contains("facebook.com")
                || ((href.contains("wikipedia") || curr.contains("wikipedia")) && href.contains("index.php"))
                || (href.contains("eclipse.org") && href.contains("download")) || href.contains("advertising.amazon.")
                || href.contains("philaathenaeum.org") || href.contains("coupons.businessinsider.")) {
            // System.out.println("CUT BITCH: " + href);
            return null;
        }
        
        if (!href.startsWith("http://") && !href.startsWith("https://")) {
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
        }

        String fullLink = sb.toString();
        
        // only checks links which are 50 directories deep
        if (fullLink.split("/").length > 53) {
            return null;
        }
        return fullLink;
    }

    public static void main(String[] args) throws IOException {
        String href = "https://www.craigslist.org/about/sites";
        
        
        Document doc = Jsoup.connect(href).timeout(5000)
                .userAgent("cis455crawler")
                .get();
        
        doc.charset(Charset.forName("UTF-8"));
//        System.out.println(doc.toString());
        
//        for (Element elt : linkElts) {
////            String rawLink = elt.attributes().get("href").trim();
////            String lang = elt.attributes().get("hreflang").trim();
//            System.out.println(elt);
//        }

        
        
        String text = null;
//////        LanguageIdentifier object = null;
        if (doc != null) {
            text = doc.text();
////            object = new LanguageIdentifier(text);
        }
        System.out.println(text);
////
//        LanguageDetector detector = new OptimaizeLangDetector().loadModels();
//        LanguageResult langResult = detector.detect(text);
//        System.out.println(langResult.getLanguage());
//        System.out.println(langResult.getConfidence());
//        System.out.println(langResult.isReasonablyCertain());
//        if (langResult != null && (!langResult.getLanguage().equals("en") && langResult.isReasonablyCertain())
//                || (langResult.getLanguage().equals("en") && !langResult.isReasonablyCertain())) {
//            System.out.println("yiff success");
//        }
//        link = link.replace("./", "");
//        link = link.replace("/'", "");
//        System.out.println(link);
    }
}
