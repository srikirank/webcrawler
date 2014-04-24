package edu.wc;

import java.io.IOException;
import edu.utilities.Constants;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.utilities.URLHelper;

public class WebCrawler extends Configured implements Tool {

	static int rowKey_Id = 10;

	public static class CrawlerMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private HTable frontTable = null;
		private HTable repoTable = null;
		private HTable crawledTable = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			repoTable = new HTable(context.getConfiguration(),
					Constants.TABLE_REPOSITORY);
			// repoTable.setAutoFlush(false);

			crawledTable = new HTable(context.getConfiguration(),
					Constants.TABLE_CRAWLED);
			// crawledTable.setAutoFlush(false);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String crawlingURL = value.toString().split(",")[1];
			try {
				Document doc;

				doc = Jsoup.connect(crawlingURL).get();
				processURL(crawlingURL, doc, context);

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);

			repoTable.close();
			frontTable.close();
		}

		private void processURL(String addURL, Document doc, Context context)
				throws NoSuchAlgorithmException, IOException,
				InterruptedException {
			URLHelper uh = new URLHelper(addURL);
			String repoKey = uh.generateKey();
			Put repoPut = new Put(Bytes.toBytes(repoKey));
			String crawledKey = uh.getTopDomain();
			Put crawledPut = new Put(Bytes.toBytes(crawledKey));

			crawledPut.add(Constants.COLUMNFAMILY_URLS_BYTES,
					Bytes.toBytes(uh.sha1()), Bytes.toBytes(addURL));
			crawledTable.put(crawledPut);

			String body = doc.body().text();
			StringBuffer sb = new StringBuffer();
			Elements links = doc.select("a");

			for (Element link : links) {
				String toCrawlURL = link.absUrl("href");
				uh.setURL(toCrawlURL);
				sb.append(uh.sha1()).append(",");
				context.write(new Text(uh.getDomain()), new Text(toCrawlURL));
			}

			String outLinks = sb.substring(0, sb.length() - 1);
			repoPut.add(Constants.COLUMNFAMILY_URL_BYTES,
					Constants.QUALIFIER_ADDRESS_BYTES, Bytes.toBytes(addURL));
			repoPut.add(Constants.COLUMNFAMILY_CONTENT_BYTES,
					Constants.QUALIFIER_BODY_BYTES, Bytes.toBytes(body));
			repoPut.add(Constants.COLUMNFAMILY_OUTGOING_BYTES,
					Constants.QUALIFIER_LINKS_BYTES, Bytes.toBytes(outLinks));
			repoTable.put(repoPut);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		int res = ToolRunner.run(conf, new WebCrawler(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = super.getConf();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		Job job = new Job(conf, "Retrieving seeds from frontier table ");

		job.setJarByClass(WebCrawler.class);
		job.setMapperClass(CrawlerMapper.class);
		job.setReducerClass(CrawlerReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("input/subset_seedurls.csv"));
		FileOutputFormat.setOutputPath(job, new Path("output"));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}