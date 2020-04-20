package edu.upenn.cis455.storage;

import java.io.File;
import java.util.Date;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.*;
import com.sleepycat.je.*;

/**
 * (MS1, MS2) A wrapper class which should include: - Set up of Berkeley DB -
 * Saving and retrieving objects including crawled docs and user information
 */

public class DBWrapper {

	public static final String CATALOG_DB = "CATALOG";
	public static final String USER_PASS_DB = "USER_PASS";
	public static final String USER_FLNAME_DB = "USER_FLNAME";
	public static final String URL_DOC_DB = "URL_DOC";
	public static final String DOC_URL_DB = "DOC_URL";
	public static final String URL_MODTIME_DB = "URL_MODTIME";
	public static final String HOST_ACCESS_DB = "HOST_ACCESS";
	public static final String URL_CONTENT_DB = "URL_CONTENT";
	public static final String URL_ROBOTS_DB = "URL_ROBOTS";
	public static final String NAME_XPATH_DB = "NAME_XPATH";
	public static final String XPATH_NAME_DB = "XPATH_NAME";
	public static final String NAME_OWNER_DB = "NAME_OWNER";
	public static final String USER_SUBSCRIPTIONS_DB = "NAME_SUBSCRIPTIONS";
	public static final String NAME_DOCS_DB = "NAME_DOCS";
	public static final String URL_VISIT_DB = "URL_VISIT";

	private String envDirectory;

	private Environment myEnv;
	private EnvironmentConfig envConfig;

	private StoredClassCatalog catalog;

	private EntryBinding<String> usernameBinding; // key
	private EntryBinding<String> passwordBinding; // val
	private EntryBinding<String> flNameBinding; // val

	private EntryBinding<String> urlBinding; // key
	private EntryBinding<String> docBinding; // val
	private EntryBinding<Date> modtimeBinding; // val
	private EntryBinding<Date> accessBinding; // val
	private EntryBinding<String> contentBinding; // val
	private EntryBinding<String> robotsBinding; // val

	private EntryBinding<String> channelNameBinding; // key and val
	private EntryBinding<String> channelXPathBinding; // val and key
	// use binding for username from earlier as value // val
	private EntryBinding<SetWrapper> subscriptionBinding; // val
	private EntryBinding<SetWrapper> docSetBinding; // val
	private EntryBinding<Date> visitBinding; // val
	private EntryBinding<SetWrapper> channelSetBinding;

	private DatabaseConfig dbConfig;
	private Database catalogDB;
	private Database userPassDB; // maps usernames to hashed passwords
	private Database userFLNameDB; // maps usernames to first+last names
	private Database urlDocDB; // maps UTF-8-encoded String to String
	private Database docURLDB; // maps String to UTF-8-encoded String
	private Database urlModtimeDB; // maps UTF-8-encoded String to Date
	private Database hostAccessDB; // maps UTF-8-encoded String to Date
	private Database urlContentDB; // maps UTF-8-encoded String to String
	private Database urlRobotsDB; // maps UTF-8-encoded String to String
	private Database nameXPathDB; // maps String to String
	private Database xPathNameDB; // maps String to String
	private Database nameOwnerDB; // maps String to String
	private Database userSubscriptionDB; // maps String to Set<String>
	private Database nameDocsDB; // maps String to Set<String>
	private Database urlVisitDB; // maps UTF-8-encoded String to Date

	protected StoredSortedMap<String, String> userPass;
	protected StoredSortedMap<String, String> userFLName;
	protected StoredSortedMap<String, String> urlDoc;
	protected StoredSortedMap<String, String> docURL;
	protected StoredSortedMap<String, Date> urlModtime;
	protected StoredSortedMap<String, Date> hostAccess;
	protected StoredSortedMap<String, String> urlContent;
	protected StoredSortedMap<String, String> urlRobots;
	protected StoredSortedMap<String, String> nameXPath;
	protected StoredSortedMap<String, SetWrapper> xPathName;
	protected StoredSortedMap<String, String> nameOwner;
	protected StoredSortedMap<String, SetWrapper> userSubscription;
	protected StoredSortedMap<String, SetWrapper> nameDocs;
	protected StoredSortedMap<String, Date> urlVisit;

	// Class 1: Base DB Wrapper
	// [x] Initialize an environment config [setTransaction(true),
	// setAllowCreate(true)]
	// [x] Initialize an environment using the environment config
	// [x] Initialize a dbconfig [setTransaction(true), setAllowCreate(true)]
	// [x] Initialize a database for the catalog
	// [x] Initialize a new stored class catalog from catalog database
	// [x] Initialize a database for each key/value pair you want to store
	// [x] Create an entryBinding for each database key and database value
	// (dynamically as a SerialBinding)
	// [x] Instantiate a StoredSortedMap for each database, entrykey, and entryvalue

	DBWrapper(String envDirectory) {
		// environment directory (absolute pathname)
		this.envDirectory = envDirectory;
		File baseDir = new File(this.envDirectory);
		baseDir.mkdir();
		if (!baseDir.isDirectory()) {
			throw new IllegalArgumentException("Database directory is invalid! Make sure it is a directory.");
		}

		// environment config
		this.envConfig = new EnvironmentConfig();
		this.envConfig.setTransactional(true);
		this.envConfig.setAllowCreate(true);

		// environment
		this.myEnv = new Environment(baseDir, this.envConfig);

		// database config
		this.dbConfig = new DatabaseConfig();
		this.dbConfig.setTransactional(true);
		this.dbConfig.setAllowCreate(true);

		// catalog
		this.catalogDB = this.myEnv.openDatabase(null, CATALOG_DB, this.dbConfig);
		this.catalog = new StoredClassCatalog(this.catalogDB);

		// key and value bindings
		this.usernameBinding = new SerialBinding<String>(this.catalog, java.lang.String.class);
		this.passwordBinding = new SerialBinding<String>(this.catalog, java.lang.String.class);
		this.flNameBinding = new SerialBinding<String>(this.catalog, java.lang.String.class);

		this.urlBinding = new SerialBinding<String>(this.catalog, java.lang.String.class);
		this.docBinding = new SerialBinding<String>(this.catalog, java.lang.String.class);
		this.modtimeBinding = new SerialBinding<Date>(this.catalog, java.util.Date.class);
		this.accessBinding = new SerialBinding<Date>(this.catalog, java.util.Date.class);
		this.contentBinding = new SerialBinding<String>(this.catalog, java.lang.String.class);
		this.robotsBinding = new SerialBinding<String>(this.catalog, java.lang.String.class);

		this.channelNameBinding = new SerialBinding<String>(this.catalog, java.lang.String.class);
		this.channelXPathBinding = new SerialBinding<String>(this.catalog, java.lang.String.class);
		this.subscriptionBinding = new SerialBinding<SetWrapper>(this.catalog, SetWrapper.class);
		this.docSetBinding = new SerialBinding<SetWrapper>(this.catalog, SetWrapper.class);
		this.visitBinding = new SerialBinding<Date>(this.catalog, java.util.Date.class);
		this.channelSetBinding = new SerialBinding<SetWrapper>(this.catalog, SetWrapper.class);

		// databases
		this.userPassDB = this.myEnv.openDatabase(null, USER_PASS_DB, this.dbConfig);
		this.userFLNameDB = this.myEnv.openDatabase(null, USER_FLNAME_DB, this.dbConfig);
		this.urlDocDB = this.myEnv.openDatabase(null, URL_DOC_DB, this.dbConfig);
		this.docURLDB = this.myEnv.openDatabase(null, DOC_URL_DB, this.dbConfig);
		this.urlModtimeDB = this.myEnv.openDatabase(null, URL_MODTIME_DB, this.dbConfig);
		this.hostAccessDB = this.myEnv.openDatabase(null, HOST_ACCESS_DB, this.dbConfig);
		this.urlContentDB = this.myEnv.openDatabase(null, URL_CONTENT_DB, this.dbConfig);
		this.urlRobotsDB = this.myEnv.openDatabase(null, URL_ROBOTS_DB, this.dbConfig);
		this.nameXPathDB = this.myEnv.openDatabase(null, NAME_XPATH_DB, this.dbConfig);
		this.xPathNameDB = this.myEnv.openDatabase(null, XPATH_NAME_DB, this.dbConfig);
		this.nameOwnerDB = this.myEnv.openDatabase(null, NAME_OWNER_DB, this.dbConfig);
		this.userSubscriptionDB = this.myEnv.openDatabase(null, USER_SUBSCRIPTIONS_DB, this.dbConfig);
		this.nameDocsDB = this.myEnv.openDatabase(null, NAME_DOCS_DB, this.dbConfig);
		this.urlVisitDB = this.myEnv.openDatabase(null, URL_VISIT_DB, this.dbConfig);

		// stored sorted maps
		this.userPass = new StoredSortedMap<String, String>(userPassDB, usernameBinding, passwordBinding, true);
		this.userFLName = new StoredSortedMap<String, String>(userFLNameDB, usernameBinding, flNameBinding, true);
		this.urlDoc = new StoredSortedMap<String, String>(urlDocDB, urlBinding, docBinding, true);
		this.docURL = new StoredSortedMap<String, String>(docURLDB, docBinding, urlBinding, true);
		this.urlModtime = new StoredSortedMap<String, Date>(urlModtimeDB, urlBinding, modtimeBinding, true);
		this.hostAccess = new StoredSortedMap<String, Date>(hostAccessDB, urlBinding, accessBinding, true);
		this.urlContent = new StoredSortedMap<String, String>(urlContentDB, urlBinding, contentBinding, true);
		this.urlRobots = new StoredSortedMap<String, String>(urlRobotsDB, urlBinding, robotsBinding, true);
		this.nameXPath = new StoredSortedMap<String, String>(nameXPathDB, channelNameBinding, channelXPathBinding,
				true);
		this.xPathName = new StoredSortedMap<String, SetWrapper>(xPathNameDB, channelXPathBinding, channelSetBinding,
				true);
		this.nameOwner = new StoredSortedMap<String, String>(nameOwnerDB, channelNameBinding, usernameBinding, true);
		this.userSubscription = new StoredSortedMap<String, SetWrapper>(userSubscriptionDB, channelNameBinding,
				subscriptionBinding, true);
		this.nameDocs = new StoredSortedMap<String, SetWrapper>(nameDocsDB, channelNameBinding, docSetBinding, true);
		this.urlVisit = new StoredSortedMap<String, Date>(urlVisitDB, urlBinding, visitBinding, true);
	}

	// getter for the database environment
	protected Environment getEnvironment() {
		return this.myEnv;
	}

	// closes databases
	protected void close() {
		this.catalogDB.close();
		this.userPassDB.close();
		this.userFLNameDB.close();
		this.urlDocDB.close();
		this.docURLDB.close();
		this.urlModtimeDB.close();
		this.hostAccessDB.close();
		this.urlContentDB.close();
		this.urlRobotsDB.close();
		this.nameXPathDB.close();
		this.xPathNameDB.close();
		this.nameOwnerDB.close();
		this.userSubscriptionDB.close();
	}
}
