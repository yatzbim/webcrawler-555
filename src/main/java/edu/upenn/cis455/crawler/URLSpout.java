package edu.upenn.cis455.crawler;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.URLInfo;

public class URLSpout implements IRichSpout {
	static Logger log = Logger.getLogger(URLSpout.class);
	String executorId = UUID.randomUUID().toString();

	static XPathCrawler instance = XPathCrawler.getInstance();

	Fields schema = new Fields("url");

	SpoutOutputCollector collector;

	static AtomicInteger idle = new AtomicInteger(0);

	public URLSpout() {
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
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void close() {
		// (TODO: Is this right?)
		instance.frontier = new LinkedBlockingQueue<>();
	}

	public static boolean isIdle() {
		return idle.get() == 0;
	}

	@Override
	public void nextTuple() {
		if (instance.shouldQuit()) {
			idle.set(0);
			return;
		}
		if (instance.frontier != null) {
			if (!instance.frontier.isEmpty()) {
				idle.incrementAndGet();
				// frontier is not empty
				String curr = instance.frontier.poll();
				
//				URLInfo uInfo = new URLInfo(curr);
//                try {
//                    if (uInfo.getHostName().contains("google") || (uInfo.getHostName().contains("wikipedia")
//                            && (uInfo.getFilePath().contains("&action=edit") || uInfo.getFilePath().contains("title=Special:")))) {
//                        idle.decrementAndGet();
//                        return;
//                    }
//				} catch (NullPointerException e) {
//				    System.out.println("NULL IN SPOUT: " + uInfo.getHostName() + " " + uInfo.getPortNo() + " " + uInfo.getFilePath());
//				    return;
//				}
//		        
//				
//				if (XPathCrawler.rds.get_crawltime(curr) > 0) {
//				    System.out.println("Already seen " + curr + " - not crawling");
//		            idle.decrementAndGet();
//		            return;
//		        }
				
				if (curr == null) {
				    return;
				}

				instance.inFlight.incrementAndGet();
				this.collector.emit(new Values<Object>(curr.trim()));
				idle.decrementAndGet();
//			} else if (XPathCrawler.allAreIdle() && instance.inFlight.get() == 0) {
//			    System.out.println("Bedtime");
//                return;
            }
		}
		Thread.yield();
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}
	
	public static void main(String[] args) {
	    URLInfo uInfo = new URLInfo("https://www.amazon.com");
	    System.out.println(uInfo.getHostName());
	    System.out.println(uInfo.getPortNo());
	    System.out.println(uInfo.getFilePath());
	}

}
