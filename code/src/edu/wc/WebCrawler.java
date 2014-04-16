package edu.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class WebCrawler extends Configured implements Tool{

	static final String FROINTER_TABLE_NAME = "frontier";
	static final String REPOSITORY_TABLE_NAME = "repository";
	static final String URL_COLUMN_FAMILY = "URL";
	static final String ADDRESS_COLUMN_NAME = "address";
	static final String CONTENT_COLUMN_NAME = "content";
	static int rowKey_Id = 10;

	public static class CrawlerMapper extends
			TableMapper<ImmutableBytesWritable, Writable> {
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result,
				Context context) throws IOException, InterruptedException {
			byte[] docIdBytes = rowKey.get();
			byte[] contentBytes = result.getValue(Bytes.toBytes(URL_COLUMN_FAMILY),
				Bytes.toBytes(ADDRESS_COLUMN_NAME));
			String content = Bytes.toString(contentBytes);
			Document doc;
			try {
				doc = Jsoup.connect(content).get();
				String body = doc.body().text();
				Elements links = doc.select("a");
				for (Element link : links) {
					byte[] rKey = Bytes.toBytes(++rowKey_Id);
					Put put = new Put(rKey);
					put.add(URL_COLUMN_FAMILY.getBytes(),
							CONTENT_COLUMN_NAME.getBytes(), Bytes.toBytes(link.text()));
						context.write(new ImmutableBytesWritable(rKey), put);
					}

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public static class CrawlerReducer extends
			TableReducer<Text, ImmutableBytesWritable, ImmutableBytesWritable> {
		@Override
		public void reduce(Text word, Iterable<ImmutableBytesWritable> freqs,
				Context context) throws IOException, InterruptedException {
			Put put = null;
			for (ImmutableBytesWritable value : freqs) {
				put = new Put(String.valueOf(++rowKey_Id).getBytes());
				put.add(URL_COLUMN_FAMILY.getBytes(),
						CONTENT_COLUMN_NAME.getBytes(), value.get());
				try {
					context.write(
							new ImmutableBytesWritable(String
									.valueOf(rowKey_Id).getBytes()), put);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

		}
	}

	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf) throws IOException {
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Scan scan = new Scan();
		scan.addColumn(URL_COLUMN_FAMILY.getBytes(),
				ADDRESS_COLUMN_NAME.getBytes());
		Job job = new Job(conf, "Retrieving seeds  from seed table ");
		job.setJarByClass(WebCrawler.class);
		TableMapReduceUtil.initTableMapperJob(FROINTER_TABLE_NAME, scan,
				CrawlerMapper.class, Text.class, Writable.class, job, true);
		TableMapReduceUtil.initTableReducerJob(REPOSITORY_TABLE_NAME,
				CrawlerReducer.class, job);
		job.setNumReduceTasks(1);
		return job;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		int res = ToolRunner.run(conf, new WebCrawler(), args);
//		Job job = configureJob(conf);
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = super.getConf();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(URL_COLUMN_FAMILY),
				Bytes.toBytes(ADDRESS_COLUMN_NAME));
		Job job = new Job(conf, "Retrieving seeds from seed table ");
		job.setJarByClass(WebCrawler.class);
		TableMapReduceUtil.initTableMapperJob(FROINTER_TABLE_NAME, scan,
				CrawlerMapper.class, ImmutableBytesWritable.class, Put.class, job, true);
		TableMapReduceUtil.initTableReducerJob(FROINTER_TABLE_NAME,
				CrawlerReducer.class, job);
		job.setNumReduceTasks(0);		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}