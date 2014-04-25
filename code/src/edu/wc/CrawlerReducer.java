package edu.wc;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.utilities.Constants;
import edu.utilities.URLHelper;
import edu.wc.WebCrawler.newURLS;

public class CrawlerReducer extends Reducer<Text, Text, NullWritable, Text> {

	private HTable crawledTable = null;
	NullWritable out = NullWritable.get();
	URLHelper urlHelper = new URLHelper();
	HashSet<String> uniqueURLs = new HashSet<String>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		crawledTable = new HTable(context.getConfiguration(),
				Constants.TABLE_CRAWLED);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		Get get = new Get(key.getBytes());
		get.addFamily(Constants.COLUMNFAMILY_URLS_BYTES);
		Result mResult = crawledTable.get(get);
		boolean newUrls = false;

		if (mResult.isEmpty()) {
			for (Text newURL : value) {
				if (uniqueURLs.add(newURL.toString())) {
					context.write(out, newURL);
					newUrls = true;
				}
			}
		} else {
			for (Text newURL : value) {
				if (uniqueURLs.add(newURL.toString())) {
					urlHelper.setURL(newURL.toString());
					try {
						if (!mResult.containsColumn(
								Constants.COLUMNFAMILY_URLS_BYTES,
								Bytes.toBytes(urlHelper.sha1()))) {
							context.write(out, newURL);
							newUrls = true;
						}
					} catch (NoSuchAlgorithmException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		if(newUrls)
		context.getCounter(newURLS.NEW).increment(1);

	}
}
