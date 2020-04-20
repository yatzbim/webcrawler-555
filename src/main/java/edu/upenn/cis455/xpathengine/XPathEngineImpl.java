package edu.upenn.cis455.xpathengine;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.helpers.DefaultHandler;

/**
 * (MS2) Implements XPathEngine to handle XPaths.
 */
public class XPathEngineImpl implements XPathEngine {

	String[] paths;

	public XPathEngineImpl() {
		// Do NOT add arguments to the constructor!!
	}

	// determines the paths to be evaluated
	public void setXPaths(String[] s) {
		/* Store the XPath expressions that are given to this method */
		if (s == null) {
			return;
		}
		this.paths = s;
	}

	// check validity of ith path
	public boolean isValid(int i) {
		/* Check which of the XPath expressions are valid */
		if (this.paths == null || this.paths[i] == null || i < 0 || i >= this.paths.length) {
			return false;
		}

		String path = this.paths[i];

		if (path.charAt(0) != '/') {
			return false;
		}

		String[] pieces = splitPath(path);

		for (String piece : pieces) {
			piece = piece.trim();
			if (piece.equals("")) {
				return false;
			}

			char[] chars = piece.toCharArray();
			int leftBrackets = 0;
			int leftParens = 0;
			boolean inQuote = false;
			for (int j = 0; j < chars.length; j++) {
				char c = chars[j];

				if (j == 0 && c == '"') {
					return false;
				}

				if (c == '"' && (j == 0 || chars[j - 1] != '\\')) {
					inQuote = !inQuote;
				}

				if (!inQuote) {
					if (j == 0 && (c == '/' || c == '=' || c == ',' || c == '@' || c == '(' || c == ')')) {
						if (c == chars[j - 1]) {
							return false;
						}
					}
					if (j != 0 && c == '/' && chars[j - 1] == '/') {
						return false;
					}

					if (c == '[') {
						leftBrackets++;
					}
					if (c == ']') {
						leftBrackets--;
					}
					if (c == '(') {
						leftParens++;
					}
					if (c == ')') {
						leftParens--;
					}
				}
			}

			if (leftBrackets != 0 || leftParens != 0 || inQuote) {
				return false;
			}

			List<String> tests = getTests(piece);
			for (String test : tests) {
				if (!isTestValid(test)) {
					return false;
				}
			}

			String name = getName(piece);
			if (name == null || !name.matches("^[A-Za-z0-9_:.-]+$")) {
				return false;
			}

		}

		return true;
	}

	// method to check the validity of a newly entered path
	// used in frontend
	public static boolean isPathValid(String path) {
		/* Check which of the XPath expressions are valid */
		if (path == null) {
			return false;
		}
		path = path.trim();
		if (path.equals("")) {
			return false;
		}

		if (path.charAt(0) != '/') {
			return false;
		}

		String[] pieces = splitPath(path);

		if (pieces.length == 0) {
			return false;
		}

		for (String piece : pieces) {
			piece = piece.trim();
			if (piece.equals("")) {
				return false;
			}

			char[] chars = piece.toCharArray();
			int leftBrackets = 0;
			int leftParens = 0;
			boolean inQuote = false;
			char prev = '\0';
			for (int j = 0; j < chars.length; j++) {
				char c = chars[j];
				if (c == '"' && (j == 0 || chars[j - 1] != '\\')) {
					inQuote = !inQuote;
				}

				if (!inQuote) {
					if (Character.isWhitespace(c)) {
						continue;
					}

					if (c == '/' || c == '=' || c == ',' || c == '@' || c == '(' || c == ')') {
						if (c == prev) {
							return false;
						}
					}
					if (j != 0 && c == '/' && chars[j - 1] == '/') {
						return false;
					}

					if (c == '[') {
						leftBrackets++;
					}
					if (c == ']') {
						leftBrackets--;
					}
					if (c == '(') {
						leftParens++;
					}
					if (c == ')') {
						leftParens--;
					}
				}
				prev = c;
			}

			if (leftBrackets != 0 || leftParens != 0 || inQuote) {
				return false;
			}

			List<String> tests = getTests(piece);
			for (String test : tests) {
				if (!isTestValid(test)) {
					return false;
				}
			}

			String name = getName(piece);
			if (name == null || !name.matches("^[A-Za-z0-9_:.-]+$")) {
				return false;
			}

		}

		return true;
	}

	// method to evaluate whether a document matches the given paths
	public boolean[] evaluate(Document d) {
		/* Check whether the document matches the XPath expressions */
		// for each XPath, call evaluate(xPath, d) and update boolean array
		// return array
		if (this.paths == null) {
			return null;
		}

		boolean[] result = new boolean[this.paths.length];
		for (int i = 0; i < this.paths.length; i++) {
			result[i] = evaluate(this.paths[i], d);
		}

		return result;
	}

	// method to evaluate a single path against a document
	private boolean evaluate(String path, Node n) {
		// break xPath into parts (with splitPath())
		// for each child of d, call evaluateRec(parts, child, 1)
		// return aggregated result
		String[] pieces = splitPath(path);

		if (path.trim().equals("/")) {
			return true;
		}

		NodeList children = n.getChildNodes();
		boolean running = false;
		for (int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);
			running = running || evaluateRec(pieces, child, 0);
		}

		return running;
	}

	// recursive helper to evaluate a document
	private boolean evaluateRec(String[] pieces, Node n, int level) {
		String curr = pieces[level];
		if (!match(curr, n)) {
			return false;
		}

		level++;

		if (level == pieces.length) {
			return true;
		}

		int numChildren = n.getChildNodes().getLength();
		boolean running = false;
		for (int i = 0; i < numChildren; i++) {
			Node child = n.getChildNodes().item(i);
			running = running || evaluateRec(pieces, child, level);
		}

		return running;
	}

	// See if current "level" of the path matches current node
	// Check if the name is a match
	// Execute each test. If any fail, return false
	private boolean match(String piece, Node node) {
		String name = getName(piece).toLowerCase();
		List<String> tests = getTests(piece);

		if (name == null || tests == null) {
			return false;
		}

		// compare name
		if (!name.equals(node.getNodeName().toLowerCase())) {
			return false;
		}

		// execute tests

		return executeTests(tests, node);
	}

	// determines whether a path test is valid
	private static boolean isTestValid(String test) {
		if (test == null) {
			return true;
		}
		test = test.trim();
		if (test.equals("")) {
			return true;
		}

		if (test.matches("^text\\(\\)=\"(.*)\"$")) { // text()
			return true;
		} else if (test.matches("^contains\\(text\\(\\),\".*\"\\)$")) { // contains
			return true;
		} else if (test.matches("^@[A-Za-z0-9_:.-]+=\"[A-Za-z0-9_:.-]+\"$")) { // attribute
			return true;
		}

		return test.matches("^[A-Za-z0-9_:.-]*(\\[.+\\])?$");
	}

	// execute a level's tests on the current node
	private boolean executeTests(List<String> tests, Node node) {
		// for each test in the list, do the following:
		// 1) if test is text(), check the node's text
		// 2) if test is contains(text(), ...), check if the node's texts contains the
		// requested text
		// 3) if test is @key="val" get node's attribute and compare
		// 4) otherwise, treat as step(s) and pass into evaluate(String xPath, Node
		// node)
		// Any of these will return a boolean. && output with running result, return
		// running result at the end

		if (node == null) {
			return false;
		}

		if (tests == null) {
			return true;
		}

		for (String test : tests) {
			if (test.matches("^text\\(\\)=\"(.*)\"$")) { // text()

				if (node.getNodeType() != Node.ELEMENT_NODE && !nodeHasTextChild(node)) {
					return false;
				}

				List<Integer> indices = textChildren(node);

				String desired = textGetText(test);
				boolean running = false;
				for (int i = 0; i < indices.size(); i++) {
					Node temp = node.getChildNodes().item(i);
					String actual = temp.getTextContent();
					running = running || desired.equals(actual);
				}

				if (running == false) {
					return false;
				}

			} else if (test.matches("^contains\\(text\\(\\),\".*\"\\)$")) { // contains
				if (node.getNodeType() != Node.ELEMENT_NODE && !nodeHasTextChild(node)) {
					return false;
				}

				List<Integer> indices = textChildren(node);

				String desired = containsGetText(test);
				boolean running = false;
				for (int i = 0; i < indices.size(); i++) {
					Node temp = node.getChildNodes().item(i);
					String actual = temp.getTextContent();
					running = running || actual.contains(desired);
				}

				if (running == false) {
					return false;
				}

			} else if (test.matches("^@.*=\".*\"$")) { // attribute
				if (!node.hasAttributes() && node.getNodeType() != Node.ELEMENT_NODE) {
					return false;
				}

				String desiredKey = getAttrKey(test);
				String desiredVal = getAttrValue(test);
				String actualVal = ((org.w3c.dom.Element) node).getAttribute(desiredKey);

				if (!desiredVal.equals(actualVal)) {
					return false;
				}

			} else { // step
				if (!evaluate(test, node)) {
					return false;
				}
			}
		}

		return true;
	}

	// determine whether a node has any text as its immediate child
	private static boolean nodeHasTextChild(Node n) {
		if (n == null) {
			return false;
		}

		NodeList children = n.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			if (children.item(i).getNodeType() == Node.TEXT_NODE) {
				return true;
			}
		}

		return false;
	}

	// gets a list of all the indices of the child nodes which are of type text
	private static List<Integer> textChildren(Node n) {
		List<Integer> indices = new LinkedList<>();
		NodeList children = n.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			if (children.item(i).getNodeType() == Node.TEXT_NODE) {
				indices.add(i);
			}
		}

		return indices;
	}

	// gets the text of a text() test
	private static String textGetText(String test) {
		// text()="..."
		if (test == null || test.length() < 9) {
			return "";
		}
		return test.substring(8, test.length() - 1);
	}

	// gets the text of a contains() test
	private static String containsGetText(String test) {
		// contains(text(),"...")
		if (test == null || test.length() < 19) {
			return "";
		}
		return test.substring(17, test.length() - 2);
	}

	// gets the name of an attribute for an @ test
	private static String getAttrKey(String test) {
		if (test == null) {
			return "";
		}
		String[] keyVal = test.split("=");

		if (keyVal.length == 1) {
			return "";
		}

		String key = keyVal[0].substring(1);
		return key;
	}

	// gets the value of an attribute for an @ test
	private static String getAttrValue(String test) {
		String[] keyVal = test.split("=");

		if (keyVal.length == 1) {
			return "";
		}

		String val = "";
		for (int i = 1; i < keyVal.length; i++) {
			val += keyVal[i];
		}
		return val.substring(1, val.length() - 1);
	}

	// ignore
	@Override
	public boolean isSAX() {
		return false;
	}

	// ignore
	@Override
	public boolean[] evaluateSAX(InputStream document, DefaultHandler handler) {
		return null;
	}

	// helper method to get the name of a level
	private static String getName(String piece) {
		if (piece == null) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		for (char c : piece.toCharArray()) {
			if (c == '[') {
				break;
			}
			sb.append(c);
		}
		return sb.toString();
	}

	// helper method to get a list of a level's tests
	private static List<String> getTests(String piece) {
		StringBuilder sb = new StringBuilder();
		List<String> intermediate = new ArrayList<>();

		char[] chars = piece.toCharArray();

		int leftBrackets = 0;
		boolean inQuote = false;
		for (int i = 0; i < chars.length; i++) {
			char c = chars[i];

			if (c == '"' && (i == 0 || chars[i - 1] != '\\')) {
				inQuote = !inQuote;
			}

			if (!inQuote) {
				if (c == '[') {
					leftBrackets++;
				}
				if (c == ']') {
					leftBrackets--;
				}
			}

			if (leftBrackets > 0) {
				if (leftBrackets != 1 || c != '[') {
					sb.append(c);
				}
			} else if (sb.length() > 0) {
				intermediate.add(sb.toString());
				sb = new StringBuilder();
			}

		}

		return intermediate;
	}

	// splits the XPath into each "level" as denoted by "/" characters
	private static String[] splitPath(String path) {
		if (path == null) {
			return null;
		}

		List<String> intermediate = new ArrayList<>();

		char[] chars = path.toCharArray();
		StringBuilder sb = new StringBuilder();

		boolean inQuote = false;
		int leftBrackets = 0;
		for (int i = 0; i < chars.length; i++) {
			char c = chars[i];

			if (c == '"' && (i == 0 || chars[i - 1] != '\\')) {
				inQuote = !inQuote;
			}

			if (!inQuote) {
				if (c == '[') {
					leftBrackets++;
				}
				if (c == ']') {
					leftBrackets--;
				}
			}
			if (Character.isWhitespace(c) && !inQuote) {
				continue;
			} else if (inQuote || leftBrackets > 0 || c != '/') {
				sb.append(c);
			} else {
				if (sb.length() == 0) {
					continue;
				}
				intermediate.add(sb.toString());
				sb = new StringBuilder();
			}
		}

		if (sb.length() != 0) {
			intermediate.add(sb.toString());
			sb = new StringBuilder();
		}

		String[] pieces = new String[intermediate.size()];
		for (int i = 0; i < pieces.length; i++) {
			pieces[i] = intermediate.get(i);
		}

		return pieces;
	}
}
