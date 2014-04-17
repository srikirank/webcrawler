package edu.utilities;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class URLTest {

	public static void main(String[] args) {
		Document doc;
		try {
			doc = Jsoup.connect("http://google.com").get();
			String body = doc.body().text();
			Elements links = doc.select("a");
			for (Element link : links) {
				System.out.println(link.absUrl("href"));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

}
