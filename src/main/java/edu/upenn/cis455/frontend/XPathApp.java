package edu.upenn.cis455.frontend;

import static spark.Spark.*;

import java.util.Set;

import edu.upenn.cis455.storage.DBAPI;
import edu.upenn.cis455.xpathengine.XPathEngineImpl;

class XPathApp {
	public static void main(String args[]) {

		if (args.length != 1) {
			System.err.println("You need to provide the path to the BerkeleyDB data store!");
			System.exit(1);
		}

		DBAPI db = new DBAPI(args[0]);

		port(8080);

		/* A simple static web page */

		// Show a form to enter username and password (should take the place of HW1MS2's
		// "/login")
		// Login button associated with form (posts to /login)
		// Button to create new account
		get("/", (request, response) -> {
			String fName = (String) (request.session().attribute("fName"));
			String lName = (String) (request.session().attribute("lName"));
			String user = request.session().attribute("username");

			StringBuilder sb = new StringBuilder();

			sb.append("<html>\n");

			sb.append("    <body>\n");
			if (fName == null || lName == null) {
				sb.append("        <h3>Please Log In</h3>");

				sb.append("        <form action=\"/login\" method=\"POST\" align=\"left\">");
				sb.append("            <p><input type=\"text\" placeholder=\"Username\" name=\"user\"/>\n");
				sb.append("            <p><input type=\"text\" placeholder=\"Password\" name=\"pass\"/>\n");
				sb.append("            <p><input type=\"submit\" value=\"Log in\"/>\n");
				sb.append("        </form>\n");

				sb.append("        <p><a href=\"/newaccount\">New Member? Click Here to Register</a>\n");
			} else {
				sb.append("        <h2>Welcome, " + fName + " " + lName + "!</h2>\n");
				sb.append("        <h3>All Channels</h3>\n");
				Set<String> channels = db.allChannels();
				for (String channel : channels) {
					sb.append("            <p>");
					Set<String> subscriptions = db.getSubscriptions(user);
					if (subscriptions == null || !subscriptions.contains(channel)) {
						sb.append(channel);
						sb.append(" -");
						sb.append(" <a href=/subscribe?name=");
						sb.append(channel);
						sb.append(">Subscribe</a>");
					} else {
						sb.append("<a href=/show?name=");
						sb.append(channel);
						sb.append(">");
						sb.append(channel);
						sb.append("</a>");
						sb.append(" -");
						sb.append(" <a href=/unsubscribe?name=");
						sb.append(channel);
						sb.append(">Unsubscribe</a> ");
					}

					String owner = db.getChannelOwner(channel);
					if (user.equals(owner)) {
						sb.append(" <a href=/delete?name=");
						sb.append(channel);
						sb.append(">Delete</a> ");
					}

					sb.append("</p>\n");
				}
				sb.append("        <h3>Create a New Channel</h3>\n");
				sb.append("        <form action=\"/create\" method=\"GET\" align=\"left\">");
				sb.append("            <p><input type=\"text\" placeholder=\"Channel Name\" name=\"name\"/>\n");
				sb.append("            <p><input type=\"text\" placeholder=\"Channel XPath\" name=\"xpath\"/>\n");
				sb.append("            <p><input type=\"submit\" value=\"Create Channel\"/>\n");
				sb.append("        </form>\n");

				sb.append("        <p><a href=\"/logout\">Log out</a>\n");
			}

			sb.append("    </body>\n");

			sb.append("</html>\n");

			return sb.toString();
		});

		// Show a form to enter new username, first+last name, password
		// Register button associated with form (posts to /register)
		get("/newaccount", (request, response) -> {
			StringBuilder sb = new StringBuilder();

			sb.append("<html>\n");

			sb.append("    <body>\n");
			sb.append("        <h3>Fill Out This Form to Register</h3>");
			sb.append("        <form action=\"/register\" method=\"POST\" align=\"left\">");
			sb.append("            <p><input type=\"text\" placeholder=\"New Username\" name=\"user\"/>\n");
			sb.append("            <p><input type=\"text\" placeholder=\"New Password\" name=\"pass\"/>\n");
			sb.append("            <p><input type=\"text\" placeholder=\"First Name\" name=\"fname\"/>\n");
			sb.append("            <p><input type=\"text\" placeholder=\"Last Name\" name=\"lname\"/>\n");
			sb.append("            <p><input type=\"submit\" value=\"Register\"/>\n");
			sb.append("        </form>\n");

			sb.append("    </body>\n");

			sb.append("</html>\n");

			return sb.toString();
		});

		// Checks whether username already exists
		// If it does exist already, return an error page (with button to return to "/")
		// If it does not exist already, add new user to database; redirect to "/"
		post("/register", (request, response) -> {
			String username = request.queryParams("user");
			String password = request.queryParams("pass");
			String fName = request.queryParams("fname");
			String lName = request.queryParams("lname");
			try {
				db.addUser(username, password, fName, lName);
			} catch (IllegalArgumentException e) {
				return e.getMessage() + " - <a href=\"/newaccount\">go back and try again!</a>";
			}

			request.session(true);
			request.session().attribute("username", username);
			request.session().attribute("fName", fName);
			request.session().attribute("lName", lName);

			response.redirect("/");

			return null;
		});

		/*
		 * Displays a login form if the user is not logged in yet (i.e., the "username"
		 * attribute in the session has not been set yet), and welcomes the user
		 * otherwise
		 */

		// Checks whether username and password match a database entry
		// If it does match, create a new session for the user; store first+last name in
		// session; redirect to "/"
		post("/login", (request, response) -> {
			String username = request.queryParams("user");
			String password = request.queryParams("pass");
			// if username (or should it be password?) is not null, username and password
			// match from database
			if (username != null) {
				// get name (first and last) from database and make it a session attribute
				String name = db.getName(username);
				String dbHash = db.getHash(username);

				byte[] hashBytes = db.getMessageDigest().digest(password.getBytes());
				StringBuilder sb = new StringBuilder();
				for (byte b : hashBytes) {
					sb.append((char) b);
				}
				String hash = sb.toString();

				if (name != null && hash != null && dbHash.equals(hash)) {
					String[] flName = name.split("\\|");
					request.session(true);
					request.session().attribute("username", username);
					request.session().attribute("fName", flName[0]);
					request.session().attribute("lName", flName[1]);

				}
			}

			response.redirect("/");

			return null;
		});

		/*
		 * Logs the user out by deleting the "username" attribute from the session. You
		 * could also invalidate the session here to get rid of the JSESSIONID cookie
		 * entirely.
		 */

		// Remove user session
		// Redirect to "/"
		get("/logout", (request, response) -> {
			request.session().removeAttribute("username");
			request.session().removeAttribute("fName");
			request.session().removeAttribute("lName");
			response.redirect("/");
			return null;
		});

		// Requires URL-encoded query param "url"
		// Returns stored document corresponding to "url"
		// If document is not found, return a 404 error
		get("/lookup", (request, response) -> {
			String queriedURL = request.queryParams("url");
			if (queriedURL == null) {
				return "No URL asked for!";
			}

			String query = db.getDocument(queriedURL);

			if (query == null) {
				response.status(404);
				return "404: Not Found";
			}

			response.header("Content-Type", db.getContentType(queriedURL));

			return query;
		});

		// create a new channel with the specified name and XPath pattern
		// with the currently logged-in user as the channel's owner
		get("/create", (request, response) -> {
			if (request.session().attribute("username") != null) {
				// active session
				String channelName = request.queryParams("name");
				String xPath = request.queryParams("xpath");
				String owner = request.session().attribute("username");
				if (channelName == null || xPath == null) {
					return "<p>Must specify a channel name and a channel path!</p><p><a href=/>Click here</a> to go home</p>";
				}
				if (!XPathEngineImpl.isPathValid(xPath)) {
					return "<p>Your channel's path is invalid!</p><p><a href=/>Click here</a> to go home</p>";
				}
				try {
					db.registerChannel(channelName, xPath, owner);
				} catch (Exception e) {
					response.status(409);
					return "<p>Error 409: Channel \"" + channelName
							+ "\" already exists!</p><p><a href=/>Click here</a> to go home</p>";
				}
				return "<p>Channel \"" + channelName + "\" registered to " + owner
						+ " successfully!</p><p><a href=/>Click here</a> to go home</p>";
			} else {
				// no active session
				response.status(401);
				return "Must <a href=/>log in</a> to register a channel!";
			}
		});

		// delete an existing channel if the creator of the channel is currently logged
		// in
		get("/delete", (request, response) -> {
			if (request.session().attribute("username") != null) {
				// active session
				String channelName = request.queryParams("name");
				String user = request.session().attribute("username");
				String owner = db.getChannelOwner(channelName);

				if (owner == null) {
					// channel does not exist
					response.status(404);
					return "<p>404: Channel \"" + channelName
							+ "\" does not exist!</p><p><a href=/>Click here</a> to go home</p>";
				}

				if (user.equals(owner)) {
					// user is allowed to delete channel
					db.deleteChannel(channelName);
					return "<p>Deletion of channel \"" + channelName
							+ "\" sucessful!</p><p><a href=/>Click here</a> to go home</p>";
				} else {
					response.status(403);
					return "<p>403: You are not the owner of this channel!</p><p><a href=/>Click here</a> to go home</p>";
					// user is not allowed to delete channel
				}
			} else {
				// no active session
				response.status(401);
				return "<p>401: Must <a href=/>log in</a> to delete your channel!</p><p><a href=/>Click here</a> to go home</p>";
			}

		});

		// should subscribe the currently logged-in user to the specified channel
		get("/subscribe", (request, response) -> {
			if (request.session().attribute("username") != null) {
				// active session
				String channelName = request.queryParams("name");
				String user = request.session().attribute("username");

				if (db.getChannelOwner(channelName) == null) {
					// channel does not exist
					response.status(404);
					return "<p>404: Channel \"" + channelName
							+ "\" does not exist!</p><p><a href=/>Click here</a> to go home</p>";
				}

				Set<String> subscriptions = db.getSubscriptions(user);
				if (subscriptions != null && subscriptions.contains(channelName)) {
					response.status(409);
					return "<p>409: User already subscribed to channel \"" + channelName
							+ "\"!</p><p><a href=/>Click here</a> to go home</p>";
				}

				db.subscribe(user, channelName);

				return "<p>Subscribed " + user + " to channel \"" + channelName
						+ "\" successfully!</p><p><a href=/>Click here</a> to go home</p>";
			} else {
				// no active session
				response.status(401);
				return "<p>401: Must <a href=/>log in</a> to subscribe to a channel!</p><p><a href=/>Click here</a> to go home</p>";
			}
		});

		// should unsubscribe the currently logged-in user from the specified channel
		get("/unsubscribe", (request, response) -> {
			if (request.session().attribute("username") != null) {
				String channelName = request.queryParams("name");
				String user = request.session().attribute("username");

				Set<String> subscriptions = db.getSubscriptions(user);
				if (subscriptions != null && !subscriptions.contains(channelName)) {
					// unsubscribe to channel you're not subscribed to
					response.status(404);
					return "<p>404: User isn't subscribed to channel \"" + channelName
							+ "\"!</p><p><a href=/>Click here</a> to go home</p>";
				}

				db.unsubscribe(user, channelName);

				return "<p>Unsubscribed " + user + " from channel \"" + channelName
						+ "\" successfully!</p><p><a href=/>Click here</a> to go home</p>";
			} else {
				// no active session
				response.status(401);
				return "<p>401: Must <a href=/>log in</a> to unsubscribe from a channel!</p><p><a href=/>Click here</a> to go home</p>";
			}
		});

		// should show the contents of the specified channel
		get("/show", (request, response) -> {
			if (request.session().attribute("username") != null) {
				String channelName = request.queryParams("name");
				String user = request.session().attribute("username");

				if (db.getChannelOwner(channelName) == null) {
					// channel does not exist
					response.status(404);
					return "<p>404: Channel \"" + channelName
							+ "\" does not exist!</p><p><a href=/>Click here</a> to go home</p>";
				}

				Set<String> subscriptions = db.getSubscriptions(user);
				if (subscriptions == null || !subscriptions.contains(channelName)) {
					// unsubscribe to channel you're not subscribed to
					response.status(404);
					return "<p>404: User isn't subscribed to channel \"" + channelName
							+ "\"!</p><p><a href=/>Click here</a> to go home</p>";
				}

				response.header("Content-Type", "text/html");
				StringBuilder sb = new StringBuilder();

				sb.append("<html>\n");
				sb.append("    <body>\n");

				sb.append("        <div class=\"channelheader\">\n");
				sb.append("Channel Name: ");
				sb.append(channelName);
				sb.append(", created by: ");
				sb.append(db.getChannelOwner(channelName));
				sb.append("\n        </div>\n");

				Set<String> docs = db.getDocs(channelName);
				if (docs == null || docs.isEmpty()) {
					sb.append(
							"        <p>No documents to show!</p>\n        <p><a href=/>Click here</a> to go home</p>");
					return sb.toString();
				}

				for (String doc : docs) {
					String link = db.getURLByDoc(doc);
					sb.append("        <p>Crawled on: ");
					sb.append(db.getLastVisit(link));
					sb.append(" Location: ");
					sb.append(link);
					sb.append("</p>\n");
					sb.append("        <div class=\"document\">\n");
					sb.append("        <xmp>");
					sb.append(doc);
					sb.append("        </xmp>");
					sb.append("\n        </div>\n");
				}
				sb.append("        <p><a href=/>Click here</a> to go home</p>");
				sb.append("    </body>\n");
				sb.append("</html>\n");

				return sb.toString();
			} else {
				// no active session
				response.status(401);
				return "401: Must <a href=/>log in</a> to see a channel!";
			}
		});
	}
}