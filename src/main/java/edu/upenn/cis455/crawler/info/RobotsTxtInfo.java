package edu.upenn.cis455.crawler.info;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** (MS1, MS2) Holds information about a robots.txt file.
  */
public class RobotsTxtInfo {
	
	private HashMap<String,ArrayList<String>> disallowedLinks;
	private HashMap<String,ArrayList<String>> allowedLinks;
	
	private HashMap<String,Integer> crawlDelays;
	private ArrayList<String> sitemapLinks;
	private ArrayList<String> userAgents;
	
	public RobotsTxtInfo(){
		disallowedLinks = new HashMap<String,ArrayList<String>>();
		allowedLinks = new HashMap<String,ArrayList<String>>();
		crawlDelays = new HashMap<String,Integer>();
		sitemapLinks = new ArrayList<String>();
		userAgents = new ArrayList<String>();
	}
	
	public RobotsTxtInfo(String robotsTxt) {
		this();
		parseRobotsTxt(robotsTxt);
	}
	
	public void parseRobotsTxt(String robotsTxt) {
		String[] groups = robotsTxt.split("\r?\n\r?\n");
		
		// for each group:
		// a) determine user agent(s)
		// b) find allowed links
		// c) find disallowed links
		// d) find crawl delay
		// e) find sitemap links
		
		for (String group : groups) {
			
			List<String> agents = new ArrayList<>();
			
			String[] lines = group.split("\r?\n");
			for (String line : lines) {
				if (line.startsWith("#")) {
					continue;
				}
				
				String[] keyVal = line.split(": ?");
				if (keyVal.length != 2) {
					continue;
				}
				
				String key = keyVal[0].trim().toLowerCase();
				String val = keyVal[1].trim().toLowerCase();
				
				switch(key) {
					case "user-agent":
						agents.add(val);
						addUserAgent(val);
						break;
					case "allow":
						for (String agent : agents) {
							addAllowedLink(agent, val);
						}
						break;
					case "disallow":
						for (String agent : agents) {
							addDisallowedLink(agent, val);
						}
						break;
					case "crawl-delay":
						for (String agent : agents) {
							addCrawlDelay(agent, Integer.parseInt(val));
						}
						break;
					case "sitemap":
						addSitemapLink(val);
				}
			}
		}
	}
	
	public boolean isAllowed(String userAgent, String path) {
		if (!containsUserAgent(userAgent)) {
			userAgent = "*";
		}
		
		List<String> allowed = getAllowedLinks(userAgent);
		List<String> disallowed = getDisallowedLinks(userAgent);
		
		return hasAllowedParent(allowed, path) || !hasDisallowedParent(disallowed, path);
	}
	
	public boolean hasAllowedParent(List<String> allowed, String path) {
		for (String allowance : allowed) {
			if (path.startsWith(allowance)) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean hasDisallowedParent(List<String> disallowed, String path) {
		for (String disallowance : disallowed) {
			if (path.startsWith(disallowance)) {
				return true;
			}
		}
		
		return false;
	}
	
	public void addDisallowedLink(String key, String value){
		if(!disallowedLinks.containsKey(key)){
			ArrayList<String> temp = new ArrayList<String>();
			temp.add(value);
			disallowedLinks.put(key, temp);
		}
		else{
			ArrayList<String> temp = disallowedLinks.get(key);
			if(temp == null)
				temp = new ArrayList<String>();
			temp.add(value);
			disallowedLinks.put(key, temp);
		}
	}
	
	public void addAllowedLink(String key, String value){
		if(!allowedLinks.containsKey(key)){
			ArrayList<String> temp = new ArrayList<String>();
			temp.add(value);
			allowedLinks.put(key, temp);
		}
		else{
			ArrayList<String> temp = allowedLinks.get(key);
			if(temp == null)
				temp = new ArrayList<String>();
			temp.add(value);
			allowedLinks.put(key, temp);
		}
	}
	
	public void addCrawlDelay(String key, Integer value){
		crawlDelays.put(key, value);
	}
	
	public void addSitemapLink(String val){
		sitemapLinks.add(val);
	}
	
	public void addUserAgent(String key){
		userAgents.add(key);
	}
	
	public boolean containsUserAgent(String key){
		return userAgents.contains(key);
	}
	
	public ArrayList<String> getDisallowedLinks(String key){
	    if (!containsUserAgent(key)) {
            key = "*";
        }
		if (disallowedLinks.get(key) == null) {
			return new ArrayList<>();
		}
		return disallowedLinks.get(key);
	}
	
	public ArrayList<String> getAllowedLinks(String key){
	    if (!containsUserAgent(key)) {
            key = "*";
        }
		if (allowedLinks.get(key) == null) {
			return new ArrayList<>();
		}
		return allowedLinks.get(key);
	}
	
	public int getCrawlDelay(String key){
		if (!containsUserAgent(key)) {
			key = "*";
		}
		
		if (crawlDelays.get(key) == null) {
			return 1;
		}
		return crawlDelays.get(key);
	}
	
	public void print(){
		for(String userAgent:userAgents){
			System.out.println("User-Agent: " + userAgent);
			ArrayList<String> dlinks = disallowedLinks.get(userAgent);
			if(dlinks != null)
				for(String dl:dlinks)
					System.out.println("Disallow: "+dl);
			ArrayList<String> alinks = allowedLinks.get(userAgent);
			if(alinks != null)
					for(String al:alinks)
						System.out.println("Allow: "+al);
			if(crawlDelays.containsKey(userAgent))
				System.out.println("Crawl-Delay: "+crawlDelays.get(userAgent));
			System.out.println();
		}
		if(sitemapLinks.size() > 0){
			System.out.println("# SiteMap Links");
			for(String sitemap:sitemapLinks)
				System.out.println(sitemap);
		}
	}
	
	public boolean crawlContainAgent(String key){
		return crawlDelays.containsKey(key);
	}
}
