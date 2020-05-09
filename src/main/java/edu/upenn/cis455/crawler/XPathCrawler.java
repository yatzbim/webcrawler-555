package edu.upenn.cis455.crawler;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis455.storage.RDS_Connection;
import test.edu.upenn.cis.stormlite.TestWordCount;

/**
 * (MS1, MS2) The main class of the crawler.
 */
public class XPathCrawler {

	// Arguments (first 3 required)
	// Arg 1: URL of starting webpage (if HTTP use URLInfo to open socket
	// if HTTPS use java.net.URL openConnection() and cast to
	// javax.net.ssl.HttpsURLConnection)
	// Arg 2: Directory containing database store
	// Arg 3: Maximum size (in megabytes) of a document to retrieve
	// Arg 4: Number of files to retrieve before stopping
	// Arg 5: Hostname for monitoring

	static Logger log = Logger.getLogger(TestWordCount.class);

	private static final String URL_SPOUT = "URL_SPOUT";
	private static final String CRAWLER_BOLT = "CRAWLER_BOLT";
	private static final String DOWNLOADER_BOLT = "DOWNLOADER_BOLT";
	private static final String FILTER_BOLT = "FILTER_BOLT";
//	private static final String CHANNEL_BOLT = "CHANNEL_BOLT";
	private static final String ROBOTSTXT_BOLT = "ROBOTSTXT_BOLT";
	public static final String TOPOLOGY_NAME = "test";
	public static final String USER_AGENT = "cis455crawler";
	public static final RDS_Connection rds = new RDS_Connection("cis455bigrds.cu7l2h9ybbex.us-east-1.rds.amazonaws.com", "3306", "db1", "admin", "cis455crawler");
	
	public static final Object accessLock = new Object();
	
	static XPathCrawler instance = null;

	static String startURL = null;
	static String baseDir = null;
	static long maxSize = -1;
	static long maxFiles = -1;
	static String monitor = "cis455.cis.upenn.edu";

	BlockingQueue<String> frontier;
	Map<String, Long> lastAccessed;

	AtomicLong headsSent;
	AtomicBoolean quit;
	AtomicInteger inFlight;
	AtomicInteger downloads;

	InetAddress host = null;
	DatagramSocket s = null;
	boolean udpWorks = false;

	static URLSpout urlSpout;
	static CrawlerBolt crawlerBolt;
	static DownloaderBolt downloaderBolt;
	static FilterBolt filterBolt;
//	static ChannelBolt channelBolt;
	static RobotsTxtBolt robotsTxtBolt;
	static LocalCluster cluster;
	static Topology topo;

	private XPathCrawler() {
		frontier = new PriorityBlockingQueue<>();
		frontier.add(startURL);

		headsSent = new AtomicLong(0);
		quit = new AtomicBoolean(false);
		inFlight = new AtomicInteger(0);
		downloads = new AtomicInteger(0);
		
		lastAccessed = new HashMap<>();

		this.udpWorks = true;
		try {
			host = InetAddress.getByName(monitor);
			s = new DatagramSocket();
		} catch (UnknownHostException e2) {
			System.err.println("Bad hostname for UDP monitoring. Proceeding without UDP monitoring");
			this.udpWorks = false;
		} catch (SocketException e) {
			System.err.println("Error creating datagram socket for UDP monitoring. Proceeding without UDP monitoring");
			this.udpWorks = false;
		}
		
	}

	public static synchronized XPathCrawler getInstance() {
		if (instance == null) {
			if (startURL == null || maxSize < 1) {
				System.err.println("Required fields haven't been set!!");
			} else {
				instance = new XPathCrawler();
			}
		}
		return instance;
	}

	public static boolean allAreIdle() {
		return URLSpout.isIdle() && CrawlerBolt.isIdle() && DownloaderBolt.isIdle() && FilterBolt.isIdle();
	}
	
	public synchronized void access(String hostPort, long nextCheck) {
	    lastAccessed.put(hostPort, nextCheck);
	}

    public synchronized void shutdown() {
        quit.set(true);
//        while (!allAreIdle())
//            ;
        AccessCleaner.flag.set(false);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
        instance.s.close();
        System.out.println("Downloads: " + downloads.get());
        System.exit(0);
    }

	public boolean shouldQuit() {
		return this.quit.get();
	}

	public static void setStartURL(String startURL) {
		XPathCrawler.startURL = startURL;
	}

	public static void setMaxSize(long megabytes) {
		if (megabytes < 1) {
			return;
		}
		XPathCrawler.maxSize = megabytes;
		XPathCrawler.maxSize *= 1000000;
	}

	public static void setMaxFiles(long numFiles) {
		if (numFiles < 1) {
			return;
		}
		XPathCrawler.maxFiles = numFiles;
	}

	public static void setMonitor(String monitor) {
		XPathCrawler.monitor = monitor;
	}

	public long incrHeadsSent() {
		return headsSent.incrementAndGet();
	}

	public long getHeadsSent() {
		return headsSent.longValue();
	}

	public static boolean isXMLDoc(String url, String contentType) {
		if (url == null || !(url.startsWith("http://") || url.startsWith("https://"))) {
			return false;
		}
		return (contentType != null && contentType.endsWith("xml")) || url.endsWith("xml");
	}

	public static void main(String args[]) {
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        
        if (args.length < 2) {
			throw new IllegalArgumentException("Too few arguments!");
		}
		
		setStartURL(args[0].trim());
		setMaxSize(Long.parseLong(args[1].trim()));

		if (args.length > 2) {
			// num of files
			setMaxFiles(Long.parseLong(args[2].trim()));
		}

		if (args.length > 3) {
			// monitor
			setMonitor(args[3].trim());
		}

		urlSpout = new URLSpout();
		crawlerBolt = new CrawlerBolt();
		downloaderBolt = new DownloaderBolt();
		filterBolt = new FilterBolt();
//		channelBolt = new ChannelBolt();
		robotsTxtBolt = new RobotsTxtBolt();

		Config config = new Config();

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(URL_SPOUT, urlSpout, 5);
		
		builder.setBolt(ROBOTSTXT_BOLT, robotsTxtBolt, 5).shuffleGrouping(URL_SPOUT);

		builder.setBolt(CRAWLER_BOLT, crawlerBolt, 5).shuffleGrouping(ROBOTSTXT_BOLT);

		builder.setBolt(DOWNLOADER_BOLT, downloaderBolt, 5).shuffleGrouping(CRAWLER_BOLT);

		builder.setBolt(FILTER_BOLT, filterBolt, 5).shuffleGrouping(DOWNLOADER_BOLT);

		cluster = new LocalCluster();
		topo = builder.createTopology();
		cluster.submitTopology(TOPOLOGY_NAME, config, topo);
		
		new AccessCleaner().start();
	}
}
