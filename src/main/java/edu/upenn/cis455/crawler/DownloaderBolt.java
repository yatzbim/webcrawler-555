package edu.upenn.cis455.crawler;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HttpsURLConnection;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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

public class DownloaderBolt implements IRichBolt {
	static Logger log = Logger.getLogger(CrawlerBolt.class);
	String executorId = UUID.randomUUID().toString();

	static XPathCrawler instance = XPathCrawler.getInstance();

	Fields schema = new Fields("links");

	OutputCollector collector;

	static AtomicInteger idle = new AtomicInteger(0);
	
	AWSDatabase aws = AWSDatabase.getInstance();

	public DownloaderBolt() {
//    	log.debug("Starting downloader bolt");
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

	@SuppressWarnings("deprecation")
	@Override
	public void execute(Tuple input) {
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
		boolean downloadable = (boolean) input.getObjectByField("downloadable");
		boolean crawlable = (boolean) input.getObjectByField("crawlable");

		URLInfo uInfo = new URLInfo(curr);

		List<String> newLinks = new LinkedList<>();
		String page = input.getStringByField("page");

		if (curr.startsWith("http://")) {
		    try {
                url = new URL(curr);
            } catch (MalformedURLException e) {
                System.err.println("Bad HTTPS URL. Continuing");
                idle.decrementAndGet();
                return;
            }

            if (downloadable) {
                // send GET request
                try {
                    httpConn = (HttpURLConnection) url.openConnection();
                } catch (IOException e) {
                    System.err.println("Error connecting to " + curr + " for GET - continuing");
                    idle.decrementAndGet();
                    return;
                }

                String doc = getDocument(httpConn, curr);
                if (doc == null) {
                    httpConn.disconnect();
                    idle.decrementAndGet();
                    return;
                }

                long lastMod = httpConn.getLastModified();
                String contentType = httpConn.getContentType();
                // download document
                // TODO: add RDS logic for lastcrawl (Here or after initial HTTP? Ask Vikas)
                if (instance.db.getDocument(curr) == null || !instance.db.getDocument(curr).equals(page)) {
                    instance.downloads.incrementAndGet();
                    System.out.println("Downloading " + curr);
                    instance.db.addDocument(curr, doc, new Date(lastMod).toGMTString(), contentType);
                    aws.savePage(curr, doc);
                }
                page = doc;
                instance.incrHeadsSent();
            }
            if (httpConn != null) {
                httpConn.disconnect();
            }

		} else if (curr.startsWith("https://")) {
			try {
				url = new URL(curr);
			} catch (MalformedURLException e) {
				System.err.println("Bad HTTPS URL. Continuing");
				idle.decrementAndGet();
				return;
			}

			if (downloadable) {
				// send GET request
				try {
					httpsConn = (HttpsURLConnection) url.openConnection();
				} catch (IOException e) {
					System.err.println("Error connecting to " + curr + " for GET - continuing");
					idle.decrementAndGet();
					return;
				}

				String doc = getDocumentHttps(httpsConn, curr);
				if (doc == null) {
					httpsConn.disconnect();
					System.out.println("Error getting document from " + curr + " - continuing");
					idle.decrementAndGet();
					return;
				}

				long lastMod = httpsConn.getLastModified();
                String contentType = httpsConn.getContentType();
                // download document
                // TODO: add RDS logic for lastcrawl (Here or after initial HTTP? Ask Vikas)
                if ((instance.db.getDocument(curr) == null || !instance.db.getDocument(curr).equals(page)) && !instance.shouldQuit()) {
                    instance.downloads.incrementAndGet();
                    System.out.println("Downloading " + curr);
                    instance.db.addDocument(curr, doc, new Date(lastMod).toGMTString(), contentType);
                    aws.savePage(curr, doc);
                }
                instance.incrHeadsSent();
			}
			if (httpsConn != null) {
				httpsConn.disconnect();
			}

		} else {
			idle.decrementAndGet();
			return;
		}

		// extract links from document
		if (crawlable) {
			// jsoup
			// extract links
			System.out.println("Extracting links from " + curr);
			Document doc = null;
			try {
				doc = Jsoup.connect(curr).get();
			} catch (IOException e) {
				System.err.println("Error connecting to " + curr + " with JSoup. Continuing");
			}

			if (doc != null) {
				Elements linkElts = doc.select("a[href]");
				for (Element elt : linkElts) {
					String rawLink = elt.attributes().get("href");
					String fullLink = linkBuilder(rawLink, curr, uInfo);
					newLinks.add(fullLink.trim());
				}
			}
		}

		aws.saveOutgoingLinks(curr, newLinks);
		
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

	// Forms HTTP request for GET based on host and URL
	// used in HTTP
	public static String getRequest(String url, String hostname) {
		StringBuilder sb = new StringBuilder();

		sb.append("GET " + url + " HTTP/1.1\r\n");
		sb.append("Host: " + hostname + "\r\n");
		sb.append("User-Agent: cis455crawler\r\n");
		sb.append("Accept: text/html, text/xml, application/xml, */*+xml\r\n\r\n");

		return sb.toString();
	}

	// If the link extracted is a local path, build up the absolute URL and return
	// it
	// Used in link extraction
	public static String linkBuilder(String ogLink, String curr, URLInfo info) {
		if (!ogLink.startsWith("http://") && !ogLink.startsWith("https://")) {

			String[] linkPieces = ogLink.split("/");

			StringBuilder linkSb = new StringBuilder();

			if (curr.startsWith("http://")) {
				linkSb.append("http://");
			} else {
				linkSb.append("https://");
			}

			if (linkPieces.length == 0) {
				return ogLink;
			}

			if (linkPieces.length == 1 || !linkPieces[0].contains(".")) {

				String filePath = info.getFilePath();
				if (filePath.endsWith(".html")) {

					String[] pathPieces = filePath.split("/");
					StringBuilder fileSb = new StringBuilder();
					for (int i = 0; i < pathPieces.length - 1; i++) {
						fileSb.append(pathPieces[i]);
						fileSb.append("/");
					}

					filePath = fileSb.toString();
				}
				linkSb.append(info.getHostName());
				linkSb.append(":");
				linkSb.append(info.getPortNo());
				linkSb.append(filePath);
				if (!filePath.endsWith("/")) {
					linkSb.append("/");
				}
				linkSb.append(ogLink);
			}

			ogLink = linkSb.toString();
		}

		return ogLink;
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
        conn.setRequestProperty("User-Agent", "cis455crawler");
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
		conn.setRequestProperty("User-Agent", "cis455crawler");
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
}
