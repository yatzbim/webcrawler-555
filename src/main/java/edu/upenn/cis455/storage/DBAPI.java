package edu.upenn.cis455.storage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.sleepycat.collections.*;
import com.sleepycat.je.DatabaseException;

// Transactional API
// Store the instance of the base wrapper that you want to use
// Initialize a TransactionRunner using the environment from the base wrapper
// For each database, create a private object class (which implements TransactionWorker) to modify that database; contains the key (and maybe value - depends on usage)
// Within those private classes, write a doWork() method which modifies database (using data from private classes and base wrapper instance)
// Create wrapper methods which run the TransactionRunner with a new instance of a private class (since it implements TransactionWorker)

public class DBAPI {
	private MessageDigest messageDigest;
	private DBWrapper dbWrapper;
	private TransactionRunner runner;

	public DBAPI(String baseDir) {
		// hash digest
		try {
			this.messageDigest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			System.err.println("Make sure the hashing algorithm exists! Should be using SHA-256.");
			System.exit(1);
		}

		// db wrapper
		this.dbWrapper = new DBWrapper(baseDir);

		// transaction runner
		this.runner = new TransactionRunner(dbWrapper.getEnvironment());
	}

	// close database
	public void close() {
		this.dbWrapper.close();
	}

	// gets digest for password matching
	public MessageDigest getMessageDigest() {
		return this.messageDigest;
	}

	// gets a user's full name
	public String getName(String username) {
		return this.dbWrapper.userFLName.get(username);
	}

	// gets a user's password hash
	public String getHash(String username) {
		return this.dbWrapper.userPass.get(username);
	}

	// gets document associated with a URL
	public String getDocument(String url) {
		return this.dbWrapper.urlDoc.get(url);
	}

	// gets last time document was modified
	public Date docLastModified(String url) {
		return this.dbWrapper.urlModtime.get(url);
	}

	// gets content type for a document
	public String getContentType(String url) {
		return this.dbWrapper.urlContent.get(url);
	}

	// gets last time a document was visited
	public Date getLastVisited(String host) {
		return this.dbWrapper.hostAccess.get(host);
	}

	// gets a host's robots.txt
	public String getRobotsTxt(String host) {
		return this.dbWrapper.urlRobots.get(host);
	}

	// gets the user who owns a channel
	public String getChannelOwner(String channelName) {
		return this.dbWrapper.nameOwner.get(channelName);
	}

	// gets a set of all of a user's subscriptions
	public HashSet<String> getSubscriptions(String user) {
		return this.dbWrapper.userSubscription.get(user);
	}

	// gets a set of a channel's matching documents
	public HashSet<String> getDocs(String channel) {
		return this.dbWrapper.nameDocs.get(channel);
	}

	// gets a set of all channels
	public Set<String> allChannels() {
		return this.dbWrapper.nameXPath.keySet();
	}

	// gets an array of all XPaths
	public String[] getXPaths() {
		Set<String> paths = (Set<String>) this.dbWrapper.nameXPath.values();
		String[] result = new String[paths.size()];
		int i = 0;
		for (String path : paths) {
			result[i] = path;
			i++;
		}

		return result;
	}

	// gets channel names by their path
	public Set<String> getNamesByPath(String xPath) {
		return this.dbWrapper.xPathName.get(xPath);
	}

	// maps document to url
	public String getURLByDoc(String doc) {
		return this.dbWrapper.docURL.get(doc);
	}

	// gets the last visit for frontend display
	@SuppressWarnings("deprecation")
	public String getLastVisit(String url) {
		// YYYY-MM-DDThh:mm:ss

		Date d = this.dbWrapper.urlVisit.get(url);
		if (d == null) {
			return null;
		}

		int year = d.getYear() + 1900;
		int month = d.getMonth() + 1;
		int day = d.getDate();
		int hour = d.getHours();
		int minute = d.getMinutes();
		int second = d.getSeconds();

		StringBuilder sb = new StringBuilder();
		sb.append(year);
		sb.append("-");
		if (month < 10) {
			sb.append(0);
		}
		sb.append(month);
		sb.append("-");
		if (day < 10) {
			sb.append(0);
		}
		sb.append(day);
		sb.append("T");
		if (hour < 10) {
			sb.append(0);
		}
		sb.append(hour);
		sb.append(":");
		if (minute < 10) {
			sb.append(0);
		}
		sb.append(minute);
		sb.append(":");
		if (second < 10) {
			sb.append(0);
		}
		sb.append(second);

		return sb.toString();
	}

	// method to add a document to a channel
	public void addDocToChannel(String channel, String page) throws Exception {
		try {
			runner.run(new AddDocToChannel(this, channel, page));
		} catch (DatabaseException e) {
			System.err.println("Database exception - not modifying the database");
		}
	}

	private class AddDocToChannel implements TransactionWorker {
		DBAPI api;
		String channel;
		String doc;

		public AddDocToChannel(DBAPI api, String channel, String doc) {
			this.api = api;
			this.channel = channel.trim();
			this.doc = doc.trim();
		}

		@Override
		public void doWork() throws Exception {
			if (this.api == null || this.channel == null || getChannelOwner(channel) == null || this.doc == null) {
				return;
			}

			SetWrapper docs = this.api.dbWrapper.nameDocs.get(channel);
			if (docs == null) {
				docs = new SetWrapper();
			}

			if (docs.contains(this.doc)) {
				return;
			}

			docs.add(doc);

			this.api.dbWrapper.nameDocs.put(channel, docs);
		}

	}

	// method to unsubscribe a user from a channel
	public void unsubscribe(String username, String channel) throws Exception {
		try {
			runner.run(new Unsubscribe(this, username, channel));
		} catch (DatabaseException e) {
			System.err.println("Database exception - not modifying the database");
		}
	}

	private class Unsubscribe implements TransactionWorker {
		DBAPI api;
		String username;
		String channel;

		public Unsubscribe(DBAPI api, String username, String channel) {
			this.api = api;
			this.username = username.trim();
			this.channel = channel.trim();
		}

		@Override
		public void doWork() throws Exception {
			if (this.api == null || this.username == null || getName(username) == null || channel == null
					|| getChannelOwner(channel) == null) {
				return;
			}

			if (this.api.dbWrapper.userSubscription.get(username) == null) {
				return;
			}

			SetWrapper subscriptions = this.api.dbWrapper.userSubscription.get(username);
			if (!subscriptions.contains(channel)) {
				return;
			}

			subscriptions.remove(channel);
			this.api.dbWrapper.userSubscription.put(username, subscriptions);
		}

	}

	// subscribe a user to a channel
	public void subscribe(String username, String channel) throws Exception {
		try {
			runner.run(new Subscribe(this, username, channel));
		} catch (DatabaseException e) {
			System.err.println("Database exception - not modifying the database");
		}
	}

	private class Subscribe implements TransactionWorker {

		DBAPI api;
		String username;
		String channel;

		public Subscribe(DBAPI api, String username, String channel) {
			this.api = api;
			this.username = username.trim();
			this.channel = channel.trim();
		}

		@Override
		public void doWork() throws Exception {
			if (this.api == null || this.username == null || this.channel == null || getName(username) == null
					|| getChannelOwner(channel) == null) {
				return;
			}

			if (this.api.dbWrapper.userSubscription.get(this.username) == null) {
				this.api.dbWrapper.userSubscription.put(this.username, new SetWrapper());
			}

			SetWrapper set = this.api.dbWrapper.userSubscription.get(this.username);
			if (set.contains(this.channel)) {
				return;
			}
			set.add(this.channel);
			this.api.dbWrapper.userSubscription.put(this.username, set);

		}

	}

	// delete a channel and its contents
	public void deleteChannel(String channelName) throws Exception {
		try {
			runner.run(new DeleteChannel(this, channelName));
		} catch (DatabaseException e) {
			System.err.println("Database exception - not modifying the database");
		}
	}

	private class DeleteChannel implements TransactionWorker {
		DBAPI api;
		String channelName;

		public DeleteChannel(DBAPI api, String channelName) {
			this.api = api;
			this.channelName = channelName.trim();
		}

		@Override
		public void doWork() throws Exception {
			if (this.api == null || this.channelName == null || !this.api.dbWrapper.nameXPath.containsKey(channelName)
					|| !this.api.dbWrapper.nameOwner.containsKey(channelName)) {
				return;
			}
			String xPath = this.api.dbWrapper.nameXPath.get(channelName).trim();
			this.api.dbWrapper.nameXPath.remove(channelName);
			this.api.dbWrapper.xPathName.remove(xPath);
			this.api.dbWrapper.nameOwner.remove(channelName);
			this.api.dbWrapper.nameDocs.remove(channelName);
		}

	}

	// register a new channel
	public void registerChannel(String channelName, String xPath, String owner) throws Exception {
		try {
			runner.run(new RegisterChannel(this, channelName, xPath, owner));
		} catch (DatabaseException e) {
			System.err.println("Database exception - not modifying the database");
		}
	}

	private class RegisterChannel implements TransactionWorker {
		DBAPI api;
		String channelName;
		String xPath;
		String owner;

		public RegisterChannel(DBAPI api, String channelName, String xPath, String owner) {
			this.api = api;
			this.channelName = channelName.trim();
			this.xPath = xPath.trim();
			this.owner = owner.trim();
		}

		@Override
		public void doWork() throws Exception {
			if (this.api == null || this.channelName == null || this.xPath == null || this.owner == null) {
				return;
			}

			if (this.api.dbWrapper.nameXPath.containsKey(channelName)
					|| this.api.dbWrapper.nameOwner.containsKey(channelName)) {
				throw new RuntimeException("Channel already exists!");
			}

			this.api.dbWrapper.nameXPath.put(channelName, xPath);

			SetWrapper set = this.api.dbWrapper.xPathName.get(xPath);
			if (set == null) {
				set = new SetWrapper();
			}
			set.add(channelName);

			this.api.dbWrapper.xPathName.put(xPath, set);
			this.api.dbWrapper.nameOwner.put(channelName, owner);
		}

	}

	// updates the last time a host was visited
	public void accessHost(String host) throws Exception {
		try {
			runner.run(new AccessHost(this, host));
		} catch (DatabaseException e) {
			System.err.println("Database exception - not modifying the database");
		}
	}

	private class AccessHost implements TransactionWorker {
		DBAPI api;
		String host;

		public AccessHost(DBAPI api, String host) {
			this.api = api;
			this.host = host.trim();
		}

		@Override
		public void doWork() throws Exception {
			if (this.api == null || this.host == null || this.host.isEmpty()) {
				return;
			}

			this.api.dbWrapper.hostAccess.put(host, new Date());
		}

	}

	// adds new robots.txt to the database
	public void addRobotsTxt(String host, String robotsTxt) throws Exception {
		try {
			runner.run(new AddRobotsTxt(this, host, robotsTxt));
		} catch (DatabaseException e) {
			System.err.println("Database exception - not modifying the database");
			return;
		}
	}

	private class AddRobotsTxt implements TransactionWorker {
		DBAPI api;
		String host;
		String robotsTxt;

		public AddRobotsTxt(DBAPI api, String host, String robotsTxt) {
			this.api = api;
			this.host = host.trim();
			this.robotsTxt = robotsTxt.trim();
		}

		@Override
		public void doWork() throws Exception {
			if (this.api == null || this.host == null || this.host.isEmpty() || this.robotsTxt == null
					|| this.robotsTxt.isEmpty()) {
				return;
			}

			this.api.dbWrapper.urlRobots.put(this.host, this.robotsTxt);
		}

	}

	// adds new user to the database
	public void addUser(String username, String password, String fName, String lName) throws Exception {
		try {
			runner.run(new AddUser(this, username, password, fName, lName));
		} catch (DatabaseException e) {
			System.err.println("Database exception - not modifying the database");
			return;
		}
	}

	private class AddUser implements TransactionWorker {
		DBAPI api;
		String username;
		String password;
		String name;

		public AddUser(DBAPI api, String username, String password, String fName, String lName) {
			this.api = api;
			this.username = username.trim();
			this.password = password.trim();
			if (fName == null || fName.equals("") || lName == null || lName.equals("")) {
				this.name = "invalid";
			} else {
				this.name = fName.trim() + "|" + lName.trim();
			}
		}

		public void doWork() {
			if (this.username == null || this.username.equals("") || this.password == null || this.password.equals("")
					|| this.name.equals("invalid")) {
				throw new IllegalArgumentException("Some or all fields of signup page are invalid");
			}

			String foundUser = this.api.dbWrapper.userPass.get(username);
			if (foundUser != null) {
				throw new IllegalArgumentException("Username already exists");
			}

			byte[] hashBytes = this.api.messageDigest.digest(password.getBytes());
			StringBuilder sb = new StringBuilder();
			for (byte b : hashBytes) {
				sb.append((char) b);
			}

			String hashedPassword = sb.toString();

			this.api.dbWrapper.userPass.put(username, hashedPassword);
			this.api.dbWrapper.userFLName.put(username, name);
		}
	}

	// adds new document to database
	public void addDocument(String url, String document, String rawModDate, String contentType) {
		try {
			runner.run(new AddDocument(this, url, document, rawModDate, contentType));
		} catch (Exception e) {
			System.err.println("Exception" + e.toString() + " when trying to add document. Doing nothing.");
			return;
		}

	}

	private class AddDocument implements TransactionWorker {
		DBAPI api;
		String url;
		String document;
		Date modDate;
		String contentType;

		public AddDocument(DBAPI api, String url, String document, String rawModDate, String contentType) {
			this.api = api;
			this.url = url.trim();
			this.document = document.trim();
			this.modDate = parseDate(rawModDate.trim());
			this.contentType = contentType.trim();
		}

		@Override
		public void doWork() {
			if (this.url == null || this.url.length() < 7 || this.document == null || this.document.equals("")) {
				return;
			}
			

			api.dbWrapper.urlDoc.put(url, document);
			api.dbWrapper.docURL.put(document, url);
			api.dbWrapper.urlModtime.put(url, modDate);
			api.dbWrapper.urlContent.put(url, contentType);
			api.dbWrapper.urlVisit.put(url, new Date());
		}

		// method to parse the date
		@SuppressWarnings("deprecation")
		private Date parseDate(String dateString) {
			// Fri, 07 Feb 2020 23:59:59 GMT
			int type = dateType(dateString);

			if (type == 1) {
				return new Date(dateString);
			} else if (type == 2) {
				return new Date(parseDate2(dateString));
			}
			return new Date(parseDate3(dateString));
		}

		// helper method to determine the date format type
		private int dateType(String datestring) {
			String[] pieces = datestring.split(" ");
			String weekday = pieces[0];

			if (weekday.charAt(weekday.length() - 1) != ',') {
				return 3;
			}

			// from here, can only be type 1 or 2

			if (weekday.charAt(weekday.length() - 2) == 'y') {
				return 2;
			}

			return 1;
		}

		// helper method to parse the date
		private String parseDate2(String dateString) {
			// Friday, 31-Dec-99 23:59:59 GMT
			String[] pieces = dateString.split(" ");

			String weekday = pieces[0].substring(0, 3);

			String[] dateBits = pieces[1].split("-");
			String day = dateBits[0];
			if (day.length() == 1) {
				day = "0" + day;
			}
			String month = dateBits[1];
			int yearNum = Integer.parseInt(dateBits[2]);
			String year = "" + yearNum;
			if (yearNum >= 70) {
				year = 19 + year;
			} else {
				year = 20 + year;
			}

			String time = pieces[2];

			String timezone = pieces[3];

			return weekday + ", " + day + " " + month + " " + year + " " + time + " " + timezone;
		}

		// helper method to parse the date
		private String parseDate3(String dateString) {
			// Fri Dec 31 23:59:59 1999
			String[] pieces = dateString.split(" ");

			String weekday = pieces[0];

			String month = pieces[1];

			String day = pieces[2];
			if (day.length() == 1) {
				day = "0" + day;
			}

			String time = pieces[3];

			String year = pieces[4];

			String timezone = "GMT";

			return weekday + ", " + day + " " + month + " " + year + " " + time + " " + timezone;
		}
	}
}
