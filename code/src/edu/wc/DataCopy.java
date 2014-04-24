package edu.wc;

import java.io.IOException;
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

public class DataCopy extends Configured implements Tool{

	static final String FROINTER_TABLE_NAME = "frontier";
	static final String REPOSITORY_TABLE_NAME = "repository";
	static final String CRAWLED_TABLE_NAME = "crawled";
	static final String URL_COLUMN_FAMILY = "URL";
	static final String OUTGOING_COLUMN_FAMILY = "outgoing";
	static final String ADDRESS_COLUMN_NAME = "address";
	static final String CONTENT_COLUMN_NAME = "content";
	static final String LINKS_COLUMN_NAME = "links";

	static int rowKey_Id = 10;

	public static class CrawlerMapper extends
			Mapper<LongWritable, Text,Text,Text> {

	    private HTable frontTable = null;
	    private HTable repoTable = null;
	    private HTable crawledTable = null;
	    private byte[] urlFamily = null;
	    private byte[] outgoingFamily = null;
	    private byte[] addressQual = null;
	    private byte[] contentQual = null;
	    private byte[] linksQual = null;

	    @Override
	    protected void setup(Context context)
	    throws IOException, InterruptedException {
	      frontTable = new HTable(context.getConfiguration(), FROINTER_TABLE_NAME);
//	      frontTable.setAutoFlush(false);
	      
	      repoTable = new HTable(context.getConfiguration(),REPOSITORY_TABLE_NAME);
//	      repoTable.setAutoFlush(false);
	      
	      crawledTable = new HTable(context.getConfiguration(),CRAWLED_TABLE_NAME);
//	      crawledTable.setAutoFlush(false);
	      urlFamily = Bytes.toBytes(URL_COLUMN_FAMILY);
	      outgoingFamily = Bytes.toBytes(OUTGOING_COLUMN_FAMILY);
	      
	      addressQual = Bytes.toBytes(ADDRESS_COLUMN_NAME);
	      contentQual = Bytes.toBytes(CONTENT_COLUMN_NAME);
	      linksQual = Bytes.toBytes(LINKS_COLUMN_NAME);
	    }

		@Override
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {
		
			String crawlingURL = value.toString();
			try {
				Document doc;
			
				doc = Jsoup.connect(crawlingURL).get();								
				addNewURL(crawlingURL,doc,context);
				

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);

			repoTable.close();
			frontTable.close();
		}
		
		private void processURL(String toCrawlURL) throws NoSuchAlgorithmException, IOException{
			URLHelper uh = new URLHelper(toCrawlURL);
			Put frontPut = new Put(Bytes.toBytes(uh.sha1()));
			frontPut.add(urlFamily, addressQual, Bytes.toBytes(toCrawlURL));

			frontTable.put(frontPut);							
		}


		private void addNewURL(String addURL, Document doc, Context context) throws NoSuchAlgorithmException, IOException, InterruptedException{
			URLHelper uh = new URLHelper(addURL);
			String urlKey = uh.generateKey();
			Put repoPut = new Put(Bytes.toBytes(urlKey));
			Put crawledPut = new Put(Bytes.toBytes(urlKey));
			
			crawledPut.add(urlFamily,addressQual,Bytes.toBytes(addURL));
			crawledTable.put(crawledPut);
			
			String body = doc.body().text();
			StringBuffer sb = new StringBuffer();  
			Elements links = doc.select("a");
			
			for (Element link : links) {
				String toCrawlURL = link.absUrl("href");
				processURL(toCrawlURL);
				uh.setURL(toCrawlURL);
				sb.append(uh.sha1()).append(",");
				context.write(new Text(uh.getDomain()), new Text(toCrawlURL));
			}					
			
			String outLinks = sb.substring(0, sb.length()-1);
	        repoPut.add(urlFamily, addressQual, Bytes.toBytes(addURL));
			repoPut.add(urlFamily, contentQual, Bytes.toBytes(body));
			repoPut.add(outgoingFamily,linksQual,Bytes.toBytes(outLinks));
			repoTable.put(repoPut);
		}
	}

	public static class CrawlerReducer extends
			Reducer<Text, Text, ImmutableBytesWritable, Writable> {

	    @Override
		public void reduce(Text domain, Iterable<Text> crawlingURLs,
				Context context) throws IOException, InterruptedException {

		}
	    
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		int res = ToolRunner.run(conf, new DataCopy(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = super.getConf();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		Job job = new Job(conf, "Loading the top 1 Million URLs");

		job.setJarByClass(DataCopy.class);
	    job.setMapperClass(ImportMapper.class);
		job.setReducerClass(CrawlerReducer.class);
		
		job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path("input"));
	    FileOutputFormat.setOutputPath(job, new Path("output"));
	    
		return job.waitForCompletion(true) ? 0 : 1;
	}
}