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
import edu.upenn.cis.stormlite.tuple.Values;

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
			if (!link.startsWith("http://") && !link.startsWith("https://")) {
				continue;
			}

			try {
				new URL(link);
			} catch (MalformedURLException e) {
				continue;
			}

			if (!isDownloadable(link)) {
				continue;
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

	// determines whether a URL is downloadable
	public static boolean isDownloadable(String url) {
		if (url == null) {
			return false;
		}

		if (!(url.endsWith(".xml") || url.endsWith(".html") || url.endsWith(".htm") || url.endsWith(".rss")
				|| hasNoExtension(url))) {
			return false;
		}

		return true;
	}

	// determines whether a URL has an extension
	private static boolean hasNoExtension(String url) {
		String[] pieces = url.split("/");

		if (pieces.length == 1) {
			return true;
		}

		String last = pieces[pieces.length - 1];

		for (char c : last.toCharArray()) {
			if (c == '.') {
				String preceding = pieces[pieces.length - 2];
				if (preceding.equals("")) {
					return true;
				}
				return false;
			}
		}

		return true;
	}

}
