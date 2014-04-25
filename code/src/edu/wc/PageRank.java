package edu.wc;

import java.io.IOException;

import edu.utilities.Constants;
import edu.utilities.URLHelper;
import edu.wc.WebCrawler.CrawlerMapper;

import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class PageRank extends Configured implements Tool {

	/*
	 * @Name: Mapper class
	 * @Description :  Scans from the HBase Repository table gets the outlinks
	 *  for each row and writes to the file the link and its out going links hash 
	 */
	public static class PageRankMapper extends TableMapper<Text, Text> {
				
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 * @Description Scans the repository table and gets the outgoing links hash for each URL
		 * 				and writes the url hash and its outgoing hash to HDFS file.
		 */
		@Override
		public void map(ImmutableBytesWritable row, Result result,	Context context)
				throws IOException, InterruptedException {
			byte[] UrlsBytes = result.getValue(Constants.COLUMNFAMILY_OUTGOING_BYTES, Constants.QUALIFIER_LINKS_BYTES);
			byte[] addressBytes = result.getValue(Constants.COLUMNFAMILY_URL_BYTES,Constants.QUALIFIER_ADDRESS_BYTES);
			String urls = Bytes.toString(UrlsBytes);
			String address = Bytes.toString(addressBytes);
			URLHelper uh = new URLHelper(address);
			String addressHash = null;
			try {
				addressHash = uh.sha1();
				context.write(new Text(addressHash),new Text(urls));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			catch (Exception e) {
				System.out.println("Exception caused by: " + e.getMessage());
			}
		}
	}

	/*
	 * Class Reducer class
	 * No implementation as of now
	 */
	public static class PageRankReducer extends
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

		@Override
		public void reduce(ImmutableBytesWritable rowKey,
				Iterable<Text> crawlingURLs, Context context)
				throws IOException, InterruptedException {

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		int res = ToolRunner.run(conf, new PageRank(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		 Scan scan = new Scan();
		    scan.addColumn(Constants.COLUMNFAMILY_OUTGOING_BYTES, Constants.QUALIFIER_LINKS_BYTES);
		    scan.addColumn(Constants.COLUMNFAMILY_URL_BYTES,Constants.QUALIFIER_ADDRESS_BYTES);
		Configuration conf = super.getConf();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		Job job = new Job(conf, "Retrieving the urls from repository ");

		job.setJarByClass(PageRank.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		TableMapReduceUtil.initTableMapperJob(
				Constants.TABLE_REPOSITORY, scan, PageRankMapper.class, Text.class, Text.class, job, true
				);
		/*
		 * Here add the output path to the hdfs file name in the first argument
		 * while giving an input to the job
		 */
		FileOutputFormat.setOutputPath(job, new Path(args[0]));  
		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
