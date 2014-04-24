package edu.wc;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.utilities.URLHelper;

public class DataCopy extends Configured implements Tool{

	static final String FROINTER_TABLE_NAME = "frontier";
	static final String REPOSITORY_TABLE_NAME = "repository";
	static final String URL_COLUMN_FAMILY = "URL";
	static final String ADDRESS_COLUMN_NAME = "address";
	static final String CONTENT_COLUMN_NAME = "content";


	public static class ImportMapper extends
	Mapper<LongWritable, Text,ImmutableBytesWritable, Writable> {

	    private HTable frontTable = null;
	    private byte[] urlFamily = null;
	    private byte[] addressQual = null;

	    @Override
	    protected void setup(Context context)
	    throws IOException, InterruptedException {
	      frontTable = new HTable(context.getConfiguration(), FROINTER_TABLE_NAME);
//	      frontTable.setAutoFlush(false);
	      
	      urlFamily = Bytes.toBytes(URL_COLUMN_FAMILY);
	      addressQual = Bytes.toBytes(ADDRESS_COLUMN_NAME);
	    }

		@Override
		protected void map(LongWritable rowKey, Text result,
				Context context) throws IOException, InterruptedException {
			try {
				String value = "http://" + result.toString().split(",")[1];
				URLHelper uh = new URLHelper(value);
				
				Put frontPut = new Put(Bytes.toBytes(uh.sha1()));
				frontPut.add(urlFamily, addressQual, Bytes.toBytes(value));
				frontTable.put(frontPut);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);

			frontTable.close();
		}
	}

	public static class CrawlerReducer extends
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

	    @Override
		public void reduce(ImmutableBytesWritable rowKey, Iterable<Text> crawlingURLs,
				Context context) throws IOException, InterruptedException {

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

		Job job = new Job(conf, "Inserting seeds to frontier table ");
		
		job.setJarByClass(DataCopy.class);
		job.setMapperClass(ImportMapper.class);
		FileInputFormat.addInputPath(job, new Path("seedurls.txt"));
		TableMapReduceUtil.initTableReducerJob(FROINTER_TABLE_NAME, CrawlerReducer.class, job);
		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}