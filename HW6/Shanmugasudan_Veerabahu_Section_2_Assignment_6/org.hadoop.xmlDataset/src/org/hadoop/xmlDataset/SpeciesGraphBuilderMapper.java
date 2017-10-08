package org.hadoop.xmlDataset;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class SpeciesGraphBuilderMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private static final String TITLE_TAG = "title";
	private static final String TEXT_TAG = "text";

	private static String nodeValue = "";

	/*
	 * Map is provided with a Text instance, which represents an XML
	 * representation of a wiki page, delineated by <page></page>. This mapper
	 * extracts and outputs the <title> and outlinks of the subject page.
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		String page = value.toString();
		List<String> outlinks = new ArrayList<String>();
		String title = "";

		try {
			DocumentBuilder dBuilder = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder();
			Document doc = dBuilder.parse(new InputSource(
					new StringReader(page)));
			if (doc.hasChildNodes()) {
				if (retriveNode(doc.getChildNodes(), TITLE_TAG)) {
					if (nodeValue.length() > 0) {
						title = nodeValue;
						reporter.setStatus(title);
					} else {
						return;
					}
				} else {
					return;
				}

				if (retriveNode(doc.getChildNodes(), TEXT_TAG)) {
					outlinks = getOutLinks(nodeValue);
				}

				StringBuilder sb = new StringBuilder();
				for (String outlink : outlinks) {
					outlink = outlink.replace(" ", "_");
					
					//lets eliminate any possible processing confusion over colons      ADDED AS A PART OF ASSIGNMENT -6
					outlink = outlink.replace(":", " ");
					outlink=outlink.replaceAll("_", " ");
					outlink=outlink.replaceAll(".&nbsp", " ");
					outlink=outlink.replaceAll("="," ");
					outlink=outlink.replaceAll("\\["," ");
					outlink=outlink.replaceAll("\\]"," ");
					outlink=outlink.replaceAll("'"," ");
					outlink=outlink.replaceAll("\\|"," ");
					outlink=outlink.replaceAll(";"," ");
					outlink=outlink.replaceAll("\\?", " ");
					outlink=outlink.replaceAll("\\/", " ");
					outlink=outlink.replaceAll("\\(", " ");
					outlink=outlink.replaceAll("\\)", " ");
					outlink=outlink.replaceAll("\\#", " ");
					outlink=outlink.replaceAll("\\!", " ");
					outlink=outlink.replaceAll("\\,", " ");
					outlink=outlink.replaceAll("\\.png", " ");
					outlink=outlink.replaceAll("\\_", " ");
					outlink=outlink.replaceAll("\\d", " ");
					sb.append(" ").append(outlink);
					
				}

				try {
					output.collect(new Text(title.trim()), new Text(sb
							.toString().trim()));
				//	System.out.println("The value of sb is : "+title);
				} catch (IOException ex) {
					Logger.getLogger(SpeciesGraphBuilderMapper.class.getName())
							.log(Level.SEVERE, null, ex);
				}

			} else {
				return;
			}

		} catch (Exception ex) {
			Logger.getLogger(SpeciesGraphBuilderMapper.class.getName()).log(
					Level.SEVERE, null, ex);
		}
	}

	private boolean retriveNode(NodeList nodeList, String nodeName) {

		for (int count = 0; count < nodeList.getLength(); count++) {

			Node tempNode = nodeList.item(count);

			if (tempNode.getNodeType() == Node.ELEMENT_NODE) {
				if (tempNode.getNodeName().equals(nodeName)) {
					nodeValue = tempNode.getTextContent();
					return true;
				}

				if (tempNode.hasChildNodes()) {
					if (retriveNode(tempNode.getChildNodes(), nodeName)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	private List<String> getOutLinks(String textStr) throws Exception {

		List<String> outlinks = new ArrayList<String>();
		boolean found = false;

		Pattern regex = Pattern
				.compile("(== Taxonavigation ==|==Taxonavigation==)([^(==)]+)");
		Matcher match = regex.matcher(textStr);
		while (match.find()) {
			String page = match.group();
			getLinks(page, outlinks);
			found = true;
		}

		regex = Pattern.compile("(== Name ==|==Name==)([^(==)]+)");
		match = regex.matcher(textStr);
		while (match.find()) {
			String page = match.group();
			getLinks(page, outlinks);
			found = true;
		}

		/*regex = Pattern.compile("(== References ==|==References==)([^(==)]+)");
		match = regex.matcher(textStr);
		while (match.find()) {
			String page = match.group();
			getLinks(page, outlinks);
			found = true;
		}*/

		if (!found) {
			outlinks.add("");
		}
		return outlinks;
	}

	private void getLinks(String page, List<String> links) {
		int start = page.indexOf("[[");
		int end;
		while (start > 0) {
			start = start + 2;
			end = page.indexOf("]]", start);
			if (end == -1) {
				break;
			}
			String toAdd = page.substring(start);
			toAdd = toAdd.substring(0, (end - start));
			links.add(toAdd);
			start = page.indexOf("[[", end + 1);
		}

		return;
	}
}