package edu.wc;

import java.io.IOException;

import edu.utilities.Constants;

import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
	long numberToCrawl = 1;

	enum newURLS {
		NEW;
	}

	public static class CrawlerMapper extends
			Mapper<LongWritable, Text, Text, Text> {

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

			String crawlingURL = "http://" + value.toString().split(",")[1];
			try {
				Document doc = null;

				doc = Jsoup.connect(crawlingURL).get();
				if (doc != null)
					processURL(crawlingURL, doc, context);

			} catch (Exception ex) {
				System.out.println("Exception while parsing file ::"
						+ ex.getMessage());
				// ex.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);

			repoTable.close();
		}

		private void processURL(String addURL, Document doc, Context context)
				throws NoSuchAlgorithmException, IOException,
				InterruptedException {
			URLHelper uh = new URLHelper(addURL);
			String repoKey = uh.generateKey();
			Put repoPut = new Put(Bytes.toBytes(repoKey));
			String crawledKey = uh.getTopDomain();
			Put crawledPut = new Put(Bytes.toBytes(crawledKey));
			String outLinks = null;
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
				context.write(new Text(uh.getTopDomain()), new Text(toCrawlURL));
			}
			if (sb.length() > 0) {
				outLinks = sb.substring(0, sb.length() - 1);
			}
			repoPut.add(Constants.COLUMNFAMILY_URL_BYTES,
					Constants.QUALIFIER_ADDRESS_BYTES, Bytes.toBytes(addURL));
			repoPut.add(Constants.COLUMNFAMILY_CONTENT_BYTES,
					Constants.QUALIFIER_BODY_BYTES, Bytes.toBytes(body));
			if (outLinks != null) {
				repoPut.add(Constants.COLUMNFAMILY_OUTGOING_BYTES,
						Constants.QUALIFIER_LINKS_BYTES,
						Bytes.toBytes(outLinks));
			}
			repoTable.put(repoPut);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = null;
		int iterNum = 0;
		int res = 0;
		String givenOutput = args[1];
		long startTime = System.currentTimeMillis();
		long totalTimeToRun = Long.parseLong(args[3])*60*1000;
		WebCrawler wc = new WebCrawler();
		while (wc.numberToCrawl > 0 && (System.currentTimeMillis() - startTime) < totalTimeToRun && res != 0) {
			conf = HBaseConfiguration.create();
			args[1] = givenOutput+"/"+iterNum;
			res = ToolRunner.run(conf, wc, args);
			args[0] = args[1];
			iterNum ++;
		}
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = new Job(conf, "Retrieving seeds from new seeds text");
		job.setJarByClass(WebCrawler.class);
		job.setMapperClass(CrawlerMapper.class);
		job.setReducerClass(CrawlerReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		numberToCrawl = job.getCounters().findCounter(newURLS.NEW).getValue();
		return job.waitForCompletion(true) ? 0 : 1;
	}
}