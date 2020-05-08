package edu.upenn.cis455.crawler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.crawler.info.URLInfo;

public class FilterBolt implements IRichBolt {
	static Logger log = Logger.getLogger(CrawlerBolt.class);
	String executorId = UUID.randomUUID().toString();

	static XPathCrawler instance = XPathCrawler.getInstance();

	Fields schema = new Fields("");

	OutputCollector collector;

	static AtomicInteger idle = new AtomicInteger(0);
	
	public FilterBolt() {
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
	public void execute(Tuple input) {
		if (instance.shouldQuit()) {
			idle.set(0);
			return;
		}
		
		idle.incrementAndGet();
		instance.inFlight.decrementAndGet();

		@SuppressWarnings("unchecked")
		List<String> links = (List<String>) input.getObjectByField("links");

		if (links == null) {
			idle.decrementAndGet();
			return;
		}
		
		for (String link : links) {
		    // make sure its an http or https link
			if (!link.startsWith("http://") && !link.startsWith("https://")) {
				continue;
			}

			// make sure it's a valid URL
            try {
                new URL(link);
            } catch (MalformedURLException e) {
                continue;
            }
            
            URLInfo uInfo = new URLInfo(link);
            try {
                if (uInfo.getHostName().contains("google") || (uInfo.getHostName().contains("wikipedia")
                        && (uInfo.getFilePath().contains("&action=edit") || uInfo.getFilePath().contains("title=Special:")))) {
                    continue;
                }
            } catch (NullPointerException e) {
                System.out.println("NULL IN FILTER: " + uInfo.getHostName() + " " + uInfo.getPortNo() + " " + uInfo.getFilePath());
                continue;
            }
            
            
            if (XPathCrawler.rds.get_crawltime(link) > 0) {
                System.out.println("Already seen " + link + " - not crawling");
                continue;
            }
            
            if (link.startsWith("http://www.imdb.com")) {
                link = link.replaceFirst("http:", "https:");
            }
			
			instance.frontier.add(link);
		}
		idle.decrementAndGet();
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
	
	public static void main(String[] args) {
	    String link = "http://www.imdb.com/pressroom/press_releases_ind/2012/12_20";
	    if (link.startsWith("http://www.imdb.com")) {
            link = link.replaceFirst("http:", "https:");
        }
	}

}
