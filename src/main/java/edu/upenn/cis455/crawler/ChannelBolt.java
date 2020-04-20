package edu.upenn.cis455.crawler;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.helper.W3CDom;
import org.jsoup.parser.Parser;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.xpathengine.XPathEngine;
import edu.upenn.cis455.xpathengine.XPathEngineFactory;

public class ChannelBolt implements IRichBolt {
	static Logger log = Logger.getLogger(CrawlerBolt.class);
	String executorId = UUID.randomUUID().toString();

	XPathCrawler instance = XPathCrawler.getInstance();

	Fields schema = new Fields();

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
	}

	@Override
	public void execute(Tuple input) {
		idle.incrementAndGet();
		instance.inFlight.decrementAndGet();
		String page = input.getStringByField("page");
		if (page == null) {
			idle.decrementAndGet();
			return;
		}
		XPathEngine x = XPathEngineFactory.getXPathEngine();
		String[] paths = instance.db.getXPaths();
		x.setXPaths(paths);
		org.jsoup.nodes.Document jDoc = Jsoup.parse(page, "", Parser.xmlParser());
		W3CDom dom = new W3CDom();
		org.w3c.dom.Document dDoc = dom.fromJsoup(jDoc);

		boolean[] matches = x.evaluate(dDoc);

		for (int i = 0; i < paths.length; i++) {
			if (matches[i]) {
				String path = paths[i];
				Set<String> channels = instance.db.getNamesByPath(path);
				for (String channel : channels) {
					try {
						instance.db.addDocToChannel(channel, page);
					} catch (Exception e) {
						System.err.println("Error adding page to channel " + channel);
					}
				}
			}
		}

		idle.decrementAndGet();
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
	}

	@Override
	public void setRouter(IStreamRouter router) {
	}

	@Override
	public Fields getSchema() {
		return this.schema;
	}

}
