package edu.upenn.cis455.crawler;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import javax.net.ssl.HttpsURLConnection;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DBAPI;

public class MS1Crawler {
	DBAPI db;
	BlockingQueue<String> frontier;
	long maxSize;
	int maxFiles;
	String monitor;

	int docsSaved;
	int headsSent;

	public MS1Crawler(String startURL, String databaseDir, long maxSize, int maxFiles, String monitor) {
		frontier = new PriorityBlockingQueue<>();
		frontier.add(startURL);

		this.db = new DBAPI(databaseDir);

		if (maxSize < 1) {
			this.maxSize = 1;
		} else {
			this.maxSize = maxSize;
		}
		this.maxSize *= 1000000;

		this.docsSaved = 0;
		this.headsSent = 0;

		this.maxFiles = maxFiles;

		this.monitor = monitor;
	}

	@SuppressWarnings("deprecation")
	public void crawl() {
		// for HTTP
		Socket httpConn;

		// for HTTPS
		URL httpsURL;
		HttpsURLConnection httpsConn = null;
		HttpsURLConnection.setFollowRedirects(false);

		String currRobotsTxt = null;

		// enable udp host monitoring
		InetAddress host = null;
		DatagramSocket s = null;
		boolean udpWorks = true;
		try {
			host = InetAddress.getByName(this.monitor);
			s = new DatagramSocket();
		} catch (UnknownHostException e2) {
			System.err.println("Bad hostname for UDP monitoring. Proceeding without UDP monitoring");
			udpWorks = false;
		} catch (SocketException e) {
			System.err.println("Error creating datagram socket for UDP monitoring. Proceeding without UDP monitoring");
			udpWorks = false;
		}

		// crawler loop
		while (!frontier.isEmpty()) {
			// checking if file max was exceeded
			if (this.headsSent >= this.maxFiles && this.maxFiles > -1) {
				break;
			}

			// current URL to crawl
			String curr = frontier.remove().trim();

			// sending udp packet
			if (udpWorks) {
				byte[] data = ("jonahmil;" + curr).getBytes();
				DatagramPacket packet = new DatagramPacket(data, data.length, host, 10455);
				try {
					s.send(packet);
				} catch (IOException e) {
					System.err.println("Error sending UDP monitoring packet for " + curr + ". Continuing");
				}
			}

			URLInfo info = new URLInfo(curr);

			if (curr.startsWith("http://")) {
				// crawl http link
				try {
					httpConn = new Socket(info.getHostName(), info.getPortNo());
				} catch (IOException e) {
					System.err.println("Error connecting to " + curr + ". Continuing");
					continue;
				}

				// get streams to send requests and get responses
				InputStream input = null;
				OutputStream output = null;
				try {
					input = httpConn.getInputStream();
					output = httpConn.getOutputStream();
				} catch (IOException e) {
					System.err.println("Error opening socket streams. Continuing");
					try {
						httpConn.close();
					} catch (IOException e1) {
						System.err.println("Error closing socket after failed to open socket streams. Continuing");
					}
					continue;
				}

				InputStreamReader in = new InputStreamReader(input);

				// get robots.txt endpoint
				String robotsTxtSite = "http://" + info.getHostName() + "/robots.txt";

				// if robots.txt has been seen before, don't make a request for it
				if (this.db.getRobotsTxt(robotsTxtSite) != null) {
					currRobotsTxt = this.db.getRobotsTxt(robotsTxtSite);
				} else {
					System.out.println("Retrieving robots.txt for http://" + info.getHostName());

					byte[] robotsReq = getRobotsTxt(robotsTxtSite, info.getHostName()).getBytes();
					try {
						output.write(robotsReq);
					} catch (IOException e) {
						System.err.println("Error getting robots.txt from host. Continuing");
						try {
							input.close();
							output.close();
							httpConn.close();
						} catch (IOException e1) {
							System.err.println(
									"Error closing streams and socket after failed robots.txt request. Continuing");
						}
						continue;
					}

					// receive robots response
					StringBuilder rResSb = new StringBuilder();
					try {
						int i = 0;
						while (i != -1) {
							i = in.read();
							if (i != -1) {
								rResSb.append((char) i);
							}

							if (!in.ready()) {
								break;
							}
						}
					} catch (IOException e) {
						// close socket and streams
						System.err.println("Error reading robots.txt from " + curr + ". Continuing");
						try {
							input.close();
							output.close();
							httpConn.close();
						} catch (IOException e1) {
							System.err.println(
									"Error closing streams and socket after failed robots.txt response. Continuing");
						}
						continue;
					}

					// parse robots.txt GET response
					String robotsResponse = rResSb.toString();

					if (robotsResponse.isEmpty()) {
						try {
							input.close();
							output.close();
							httpConn.close();
						} catch (IOException e1) {
						}
						continue;
					}

					String[] robotsResPieces = robotsResponse.split("\r\n\r\n");
					if (robotsResPieces.length < 2) {
						frontier.add(curr);
						try {
							input.close();
							output.close();
							httpConn.close();
						} catch (IOException e1) {
						}
						continue;
					}

					StringBuilder txtSb = new StringBuilder();
					for (int j = 1; j < robotsResPieces.length; j++) {
						txtSb.append(robotsResPieces[j]);
						txtSb.append("\r\n");
					}

					currRobotsTxt = txtSb.toString();

					// save robots.txt in case you need it later
					try {
						this.db.addRobotsTxt(robotsTxtSite, currRobotsTxt);
					} catch (Exception e) {
						System.err.println("Issue saving robots.txt for " + info.getHostName() + " - Continuing");
					}
				}

				RobotsTxtInfo rInfo = new RobotsTxtInfo(currRobotsTxt);

				// determines whether enough time has passed to allow you to crawl a website
				int delay = rInfo.getCrawlDelay("cis455crawler");
				boolean delayAllows = true;
				long now = new Date().getTime() / 1000;
				if (this.db.getLastVisited(info.getHostName()) != null) {
					long then = this.db.getLastVisited(info.getHostName()).getTime() / 1000;
					if (now - then < delay) {
						delayAllows = false;
					}
				}

				// reenqueues url if delay doesn't allow for crawling right now
				if (!delayAllows) {
					frontier.add(curr);
					try {
						input.close();
						output.close();
						httpConn.close();
					} catch (IOException e1) {
						System.err.println("Error closing streams and socket after crawl delay reenqueue. Continuing");
					}
					continue;
				}

				// since we've waited long enough, update the last access time
				try {
					this.db.accessHost(info.getHostName());
				} catch (Exception e2) {
					System.err.println(
							"Error updating robots.txt access time for " + info.getHostName() + ". Continuing");
				}

				// next crawler checks whether the crawler is allowed on the current URL
				if (!rInfo.isAllowed("cis455crawler", info.getFilePath())) {
					System.out.println("Not permitted to crawl " + curr + ". Continuing");
					try {
						input.close();
						output.close();
						httpConn.close();
					} catch (IOException e1) {
						System.err.println("Error closing streams and socket after disallowed crawl. Continuing");
					}
					continue;
				}

				// make head request
				byte[] headRequest = headRequest(curr, info.getHostName()).getBytes();
				try {
					this.headsSent++;
					output.write(headRequest);
				} catch (IOException e) {
					System.err.println("Error writing HEAD request to " + curr + ". Continuing");
					try {
						input.close();
						output.close();
						httpConn.close();
					} catch (IOException e1) {
						System.err.println("Error closing streams and socket after failed HEAD request. Continuing");
					}
					continue;
				}

				// receive head response
				StringBuilder hResSb = new StringBuilder();
				try {
					int i = 0;
					while (i != -1) {
						i = in.read();
						if (i != -1) {
							hResSb.append((char) i);
						}
						if (!in.ready()) {
							break;
						}
					}

				} catch (IOException e) {
					// close socket and streams
					System.err.println("Error reading HEAD response. Continuing");
					try {
						input.close();
						output.close();
						httpConn.close();
					} catch (IOException e1) {
						System.err.println("Error closing streams and socket after failed HEAD response. Continuing");
					}
					continue;
				}

				String headResponse = hResSb.toString();
				int headResLength = headResponse.length();

				// begin parsing response
				String[] headHeaders = headResponse.split("\r\n");

				// make sure size parameter is met
				// make sure content type is met
				boolean rightSize = false;
				boolean downloadable = false;
				boolean crawlable = false;

				String resLine = headHeaders[0].trim();
				String[] resLinePieces = resLine.split(" ");
				int hCode = Integer.parseInt(resLinePieces[1]);

				if (hCode >= 300 && hCode < 400) {
					// handles redirect (3xx) codes
					String location = getLocation(headResponse);
					frontier.add(location);
					System.out
							.println("Redirection of type " + hCode + " found from " + curr + " to " + location + ".");
					try {
						input.close();
						output.close();
						httpConn.close();
					} catch (IOException e1) {
						System.err.println("Error closing streams and socket after redirect. Continuing");
					}
					continue;
				} else if (hCode >= 400) {
					// handles bad request (4xx, 5xx) codes
					System.err.print("Error code " + hCode + " produced from HEAD request to " + curr + ". ");
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
					try {
						input.close();
						output.close();
						httpConn.close();
					} catch (IOException e1) {
						System.err.println("Error closing streams and socket after bad response. Continuing");
						continue;
					}
					continue;
				}

				// determines content length and content type
				// checks against max size and allowable content types
				long contentLength = -1;
				String contentType = null;
				for (String header : headHeaders) {
					String[] pieces = header.split(": ?");
					if (pieces.length != 2) {
						continue;
					}

					String field = pieces[0].trim();
					String value = pieces[1].trim();

					if (field.equalsIgnoreCase("Content-Length")) {
						// size parameter
						// convert to megabytes and compare to max
						contentLength = Long.parseLong(value);

						rightSize = (contentLength <= this.maxSize);

					} else if (field.equalsIgnoreCase("Content-Type")) {
						// content type parameter
						// feed into isDownloadable
						downloadable = isDownloadable(curr, value);
						crawlable = isCrawlable(curr, value);
						contentType = value;
					}
				}

				String rawModDate = getLastModified(headResponse);

				// check if we've crawled a URL before
				String docInDB = db.getDocument(curr);

				// case for if we've crawled the document before
				if (docInDB != null) {
					// determine whether doc needs to be downloaded again
					Date docLastModified = db.docLastModified(curr);
					// this before that -> negative
					// this after that -> positive
					if (docLastModified.compareTo(new Date(rawModDate)) >= 0) {
						// don't download, just extract links
						System.err
								.println("Not downloading " + curr + " - we already have the most up-to-date version");
						downloadable = false;
					}
				}

				// content is okay to download
				if (rightSize && downloadable) {
					// send GET request
					byte[] getRequest = getRequest(curr, info.getHostName()).getBytes();
					try {
						output.write(getRequest);
					} catch (IOException e) {
						System.err.println("Error writing HEAD request to host. Continuing");
						try {
							input.close();
							output.close();
							httpConn.close();
						} catch (IOException e1) {
							System.err
									.println("Error closing streams and socket after failed HEAD request. Continuing");
							continue;
						}
						continue;
					}

					// receive GET response
					StringBuilder gResSb = new StringBuilder();
					try {
						int i = 0;
						long cLenModifiable = contentLength + headResLength;
						while (i != -1 && cLenModifiable > 0) {
							i = in.read();
							if (i != -1) {
								gResSb.append((char) i);
								cLenModifiable--;
							}
						}

					} catch (IOException e) {
						// close socket and streams
						System.err.println("Error reading GET response. Continuing");
						try {
							input.close();
							output.close();
							httpConn.close();
						} catch (IOException e1) {
							System.err
									.println("Error closing streams and socket after failed GET response. Continuing");
							continue;
						}
						continue;
					}
					String getResponse = gResSb.toString();

					// retrieve GET reesponse body
					String[] headBody = getResponse.split("\r\n\r\n");
					if (headBody.length < 2) {
						System.err.println("Badly formed GET response from " + curr + ". Continuing");
						try {
							input.close();
							output.close();
							httpConn.close();
						} catch (IOException e1) {
							System.err.println(
									"Error closing streams and socket after badly formed GET response. Continuing");
							continue;
						}
						continue;
					}

					// double check status code
					String[] getHeaderLines = headBody[0].split("\r\n");
					String getResLine = getHeaderLines[0];
					String[] getResLinePieces = getResLine.split(" ");
					int gCode = Integer.parseInt(getResLinePieces[1]);
					if (gCode >= 400) {
						System.err.println("Error GET response. Continuing");
						try {
							input.close();
							output.close();
							httpConn.close();
						} catch (IOException e1) {
							System.err.println("Error closing streams and socket after error GET response. Continuing");
							continue;
						}
						continue;
					}

					StringBuilder bodySb = new StringBuilder();
					for (int j = 1; j < headBody.length; j++) {
						bodySb.append(headBody[j]);
						bodySb.append("\r\n");
					}

					String body = bodySb.toString();

					// Actually save document
					System.out.println("Downloading " + curr);
					db.addDocument(curr, body, rawModDate, contentType);
					this.docsSaved++;
				} else if (!rightSize) {
					System.out.println("Not downloading " + curr + " - too large");
				}

				// extract links from the page
				if (crawlable) {
					System.out.println("Extracting links from " + curr);
					Document doc = null;
					try {
						doc = Jsoup.connect(curr).get();
					} catch (IOException e) {
						System.err.println(e.getMessage());
						System.err.println("Error connecting to " + curr + " with JSoup. Continuing");
					}

					if (doc != null) {
						Elements linkElts = doc.select("a[href]");
						for (Element elt : linkElts) {
							String rawLink = elt.attributes().get("href");
							String fullLink = linkBuilder(rawLink, curr, info);
							frontier.add(fullLink);
						}
					}
				}

				try {
					input.close();
					output.close();
					httpConn.close();
				} catch (IOException e1) {
					System.err.println("Error closing streams and socket at end of loop. Continuing");
					continue;
				}

			} else if (curr.startsWith("https://")) {
				// case to crawl HTTPS links

				// establish connection to URL
				try {
					httpsURL = new URL(curr);
				} catch (MalformedURLException e) {
					System.err.println("Bad HTTPS URL. Continuing");
					continue;
				}
				try {
					httpsConn = (HttpsURLConnection) httpsURL.openConnection();
				} catch (IOException e) {
					System.err.println("Problem connecting to HTTPS URL. Continuing");
					continue;
				}

				// retrieve robots.txt
				String robotsTxtSite = "https://" + info.getHostName() + "/robots.txt";
				if (this.db.getRobotsTxt(robotsTxtSite) != null) {
					// if we've seen the robots.txt before
					currRobotsTxt = this.db.getRobotsTxt(robotsTxtSite);
				} else {
					// if we've never seen the robots.txt before
					currRobotsTxt = getHttpsRobotsTxt(info.getHostName());
					if (currRobotsTxt == null) {
						frontier.add(curr);
						httpsConn.disconnect();
						continue;
					}

					try {
						this.db.addRobotsTxt(robotsTxtSite, currRobotsTxt);
					} catch (Exception e) {
						System.err.println("Issue saving robots.txt for " + info.getHostName() + " - Continuing");
					}
				}

				RobotsTxtInfo rInfo = new RobotsTxtInfo(currRobotsTxt);

				// determine crawl delay
				int delay = rInfo.getCrawlDelay("cis455crawler");
				boolean delayAllows = true;
				long now = new Date().getTime() / 1000;
				if (this.db.getLastVisited(info.getHostName()) != null) {
					long then = this.db.getLastVisited(info.getHostName()).getTime() / 1000;
					if (now - then < delay) {
						delayAllows = false;
					}
				}

				// check whether crawl delay allows for crawling
				if (!delayAllows) {
					frontier.add(curr);
					httpsConn.disconnect();
					continue;
				}

				// since we've waited long enough, update the last access
				try {
					this.db.accessHost(info.getHostName());
				} catch (Exception e2) {
					System.err.println(
							"Error updating robots.txt access time for " + info.getHostName() + ". Continuing");
				}

				// checks to see if the crawler is allowed to crawl this URL
				if (!rInfo.isAllowed("cis455crawler", info.getFilePath())) {
					System.out.println("Not permitted to crawl " + curr + ". Continuing");
					httpsConn.disconnect();
					continue;
				}

				// send HEAD request
				try {
					httpsConn.setRequestMethod("HEAD");
				} catch (ProtocolException e1) {
					System.err.println("Error setting robots.txt request method for " + curr);
					httpsConn.disconnect();
					continue;
				}
				httpsConn.setRequestProperty("Host", info.getHostName());
				httpsConn.setRequestProperty("User-Agent", "cis455crawler");
				httpsConn.setRequestProperty("Accept", "text/html, text/xml, application/xml, */*+xml");

				this.headsSent++;

				int hCode = 0;
				try {
					hCode = httpsConn.getResponseCode();
				} catch (IOException e) {
					System.err.println("Error getting HEAD response code for " + curr);
				}

				if (hCode >= 300 && hCode < 400) {
					// handle redirect (3xx) response code
					String location = httpsConn.getHeaderField("Location");
					frontier.add(location);
					System.out.println("Redirection of type " + hCode + " found from " + curr + " to " + location);
					httpsConn.disconnect();
					continue;
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
					continue;
				}

				boolean downloadable = true;

				// determine content length
				int contentLength = httpsConn.getContentLength();
				if (contentLength > this.maxSize) {
					downloadable = false;
				}

				// determine content type
				String contentType = httpsConn.getContentType();
				if (!(contentType.endsWith("/html") || contentType.endsWith("/xml") || contentType.endsWith("+xml"))) {
					downloadable = false;
				}
				boolean crawlable = contentType.endsWith("/html");

				// determine whether document has already been crawled
				long lastMod = httpsConn.getLastModified();

				// determine whether doc has already been downloaded
				String docInDB = db.getDocument(curr);
				if (docInDB != null) {
					// determine whether doc needs to be downloaded again
					Date docLastModified = db.docLastModified(curr);
					// this before that -> negative
					// this after that -> positive
					if (docLastModified.compareTo(new Date(lastMod)) >= 0) {
						System.err
								.println("Not downloading " + curr + " - we already have the most up-to-date version");
						downloadable = false;
					}
				}

				// document is okay to download
				if (downloadable) {
					// send GET request
					httpsConn.disconnect();
					try {
						httpsConn = (HttpsURLConnection) httpsURL.openConnection();
					} catch (IOException e) {
						System.err.println("Error connecting to " + curr + " for GET - continuing");
						httpsConn.disconnect();
						continue;
					}

					String doc = getDocument(httpsConn, curr);
					if (doc == null) {
						httpsConn.disconnect();
						continue;
					}

					// download document
					System.out.println("Downloading " + curr);
					db.addDocument(curr, doc, new Date(lastMod).toGMTString(), contentType);

					docsSaved++;
				}

				// extract links from document
				if (crawlable) {
					// jsoup
					// extract links
					if (crawlable) {
						System.out.println("Extracting links from " + curr);
						Document doc = null;
						try {
							doc = Jsoup.connect(curr).get();
						} catch (IOException e) {
							System.err.println(e.getMessage());
							System.err.println("Error connecting to " + curr + " with JSoup. Continuing");
						}

						if (doc != null) {
							Elements linkElts = doc.select("a[href]");
							for (Element elt : linkElts) {
								String rawLink = elt.attributes().get("href");
								String fullLink = linkBuilder(rawLink, curr, info);
								frontier.add(fullLink);
							}
						}
					}
				}
				httpsConn.disconnect();
			} else {
				continue;
			}
		}
		// close database
		db.close();

		System.out.println("Saved " + this.docsSaved + " documents.");
	}

	// Get robots.txt for a new HTTPS host
	public static String getHttpsRobotsTxt(String robotsTxtHost) {
		URL url = null;
		try {
			url = new URL("https://" + robotsTxtHost + "/robots.txt");
		} catch (MalformedURLException e2) {
			System.err.println("Bad URL: https://" + robotsTxtHost + "/robots.txt");
			return null;
		}

		HttpsURLConnection httpsConn = null;
		try {
			httpsConn = (HttpsURLConnection) url.openConnection();
		} catch (IOException e2) {
			System.err.println("Failed to connect to https://" + robotsTxtHost + "/robots.txt");
			return null;
		}

		try {
			httpsConn.setRequestMethod("GET");
		} catch (ProtocolException e1) {
			System.err.println("Error setting robots.txt request method for " + robotsTxtHost);
			httpsConn.disconnect();
			return null;
		}
		httpsConn.setRequestProperty("Host", robotsTxtHost);
		httpsConn.setRequestProperty("User-Agent", "cis455crawler");
		httpsConn.setRequestProperty("Accept", "text/plain");

		int code;
		try {
			code = httpsConn.getResponseCode();
		} catch (IOException e) {
			System.err.println("Error getting robots.txt response code for " + robotsTxtHost);
			httpsConn.disconnect();
			return null;
		}
		if (code >= 400) {
			System.out.print("https://" + robotsTxtHost + "/robots.txt: ");
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
			httpsConn.disconnect();
			return null;
		}

		StringBuilder sb = new StringBuilder();

		InputStream input;
		try {
			input = httpsConn.getInputStream();
		} catch (IOException e) {
			System.err.println("Error getting robots.txt input stream for " + robotsTxtHost);
			httpsConn.disconnect();
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
			httpsConn.disconnect();
			return null;
		}

		String robotsResponse = sb.toString();

		if (robotsResponse.isEmpty()) {
			try {
				input.close();
			} catch (IOException e) {
				System.err.println("Error closing robots.txt input stream for " + robotsTxtHost);
			}
			httpsConn.disconnect();
			return null;
		}

		httpsConn.disconnect();

		return robotsResponse;
	}

	// get the document for an HTTPS link
	public static String getDocument(HttpsURLConnection httpsConn, String url) {
		try {
			httpsConn.setRequestMethod("GET");
		} catch (ProtocolException e1) {
			System.err.println("Error setting GET request method for " + url);
			return null;
		}

		URLInfo info = new URLInfo(url);

		httpsConn.setRequestProperty("Host", info.getHostName());
		httpsConn.setRequestProperty("User-Agent", "cis455crawler");
		httpsConn.setRequestProperty("Accept", "text/plain");

		int code;
		try {
			code = httpsConn.getResponseCode();
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
			input = httpsConn.getInputStream();
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

			if (!linkPieces[0].contains(".") || linkPieces.length == 1) {

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

	// Determines the last time the document at a URL was modified
	// used in HTTP
	@SuppressWarnings("deprecation")
	public static String getLastModified(String headResponse) {
		String[] headers = headResponse.split("\r\n");
		for (String header : headers) {
			String[] pieces = header.split(": ?[^\\da-zA-Z]");
			if (pieces.length != 2) {
				continue;
			}

			String field = pieces[0].trim();
			String value = pieces[1].trim();

			if (field.equalsIgnoreCase("Last-Modified")) {
				return value;
			}
		}

		return new Date(0).toGMTString();
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

	// Forms HTTP request for HEAD based on host and URL
	// used in HTTP
	public static String headRequest(String url, String hostname) {
		StringBuilder sb = new StringBuilder();

		sb.append("HEAD " + url + " HTTP/1.1\r\n");
		sb.append("Host: " + hostname + "\r\n");
		sb.append("User-Agent: cis455crawler\r\n");
		sb.append("Accept: text/html, text/xml, application/xml, */*+xml\r\n\r\n");

		return sb.toString();
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

	// Forms HTTP request to GET robots.txt
	// used in HTTP
	public static String getRobotsTxt(String url, String hostname) {
		StringBuilder sb = new StringBuilder();

		sb.append("GET " + url + " HTTP/1.1\r\n");
		sb.append("Host: " + hostname + "\r\n");
		sb.append("User-Agent: cis455crawler\r\n");
		sb.append("Accept: text/plain\r\n\r\n");

		return sb.toString();
	}

	// determines whether a URL is crawlable
	public static boolean isCrawlable(String url, String contentType) {
		if (url == null) {
			return false;
		}

		if (!(url.startsWith("http://") || url.startsWith("https://"))) {
			return false;
		}

		if (!(url.endsWith(".html") || url.endsWith(".htm") || hasNoExtension(url))
				|| !contentType.equalsIgnoreCase("text/html")) {
			return false;
		}

		return true;
	}

	// determines whether a URL is downloadable
	public static boolean isDownloadable(String url, String contentType) {
		if (url == null) {
			return false;
		}

		if (!(url.endsWith(".xml") || url.endsWith(".html") || url.endsWith(".htm") || url.endsWith(".rss")
				|| hasNoExtension(url)) || !downloadableContentType(contentType)) {
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

	// determines whether content type allows for downloading
	private static boolean downloadableContentType(String contentType) {
		return contentType.equalsIgnoreCase("text/html") || contentType.equalsIgnoreCase("text/xml")
				|| contentType.equalsIgnoreCase("application/xml") || contentType.endsWith("+xml");
	}
}
