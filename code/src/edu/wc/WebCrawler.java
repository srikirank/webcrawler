package edu.wc;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.utilities.URLHelper;

public class WebCrawler extends Configured implements Tool{

	static final String FROINTER_TABLE_NAME = "frontier";
	static final String REPOSITORY_TABLE_NAME = "repository";
	static final String URL_COLUMN_FAMILY = "URL";
	static final String ADDRESS_COLUMN_NAME = "address";
	static final String CONTENT_COLUMN_NAME = "content";

	static int rowKey_Id = 10;

	public static class CrawlerMapper extends
			TableMapper<ImmutableBytesWritable, Text> {

	    private HTable frontTable = null;
	    private HTable repoTable = null;
	    private byte[] urlFamily = null;
	    private byte[] addressQual = null;
	    private byte[] contentQual = null;

	    @Override
	    protected void setup(Context context)
	    throws IOException, InterruptedException {
	      frontTable = new HTable(context.getConfiguration(), FROINTER_TABLE_NAME);
//	      frontTable.setAutoFlush(false);
	      
	      repoTable = new HTable(context.getConfiguration(),REPOSITORY_TABLE_NAME);
//	      repoTable.setAutoFlush(false);
	      urlFamily = Bytes.toBytes(URL_COLUMN_FAMILY);
	      addressQual = Bytes.toBytes(ADDRESS_COLUMN_NAME);
	      contentQual = Bytes.toBytes(CONTENT_COLUMN_NAME);
	    }

		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result,
				Context context) throws IOException, InterruptedException {
			byte[] contentBytes = result.getValue(Bytes.toBytes(URL_COLUMN_FAMILY),
				Bytes.toBytes(ADDRESS_COLUMN_NAME));
			String crawlingURL = Bytes.toString(contentBytes);
			
			Document doc;
			try {
				doc = Jsoup.connect(crawlingURL).get();				
				String body = doc.body().text();				
				addNewURLtoRepository(crawlingURL, body);
				
				Elements links = doc.select("a");
				
				for (Element link : links) {

					String toCrawlURL = link.absUrl("href");
					processURL(toCrawlURL);
				}
				context.write(rowKey, new Text(crawlingURL));
				// finishCrawling(docIdBytes, crawlingURL);

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		private void processURL(String toCrawlURL) throws NoSuchAlgorithmException, IOException{
			URLHelper uh = new URLHelper(toCrawlURL);
			if(!isCrawled(toCrawlURL)){
				if(!isTrackedFrontier(toCrawlURL)){
					Put frontPut = new Put(Bytes.toBytes("toCrawl-" + uh.sha1()));
					frontPut.add(urlFamily, addressQual, Bytes.toBytes(toCrawlURL));

					frontTable.put(frontPut);							
				}
			}						
		}

		private boolean isCrawled(String checkURL) throws IOException, NoSuchAlgorithmException{
			URLHelper uh = new URLHelper(checkURL);

			Get repoGet = new Get(Bytes.toBytes(uh.generateKey()));
			Result repoRow = repoTable.get(repoGet);
			return !(repoRow.isEmpty());		
		}

		private void addNewURLtoRepository(String addURL, String body) throws NoSuchAlgorithmException, IOException{
			URLHelper uh = new URLHelper(addURL);
			String urlKey = uh.generateKey();
					
	        Put repoPut = new Put(Bytes.toBytes(urlKey));
	        repoPut.add(urlFamily, addressQual, Bytes.toBytes(addURL));
			repoPut.add(urlFamily, contentQual, Bytes.toBytes(body));
			repoTable.put(repoPut);
		}

		private boolean isTrackedFrontier(String checkURL) throws NoSuchAlgorithmException, IOException{
			URLHelper uh = new URLHelper(checkURL);

			Get frontGet1 = new Get(Bytes.toBytes("toCrawl-" + uh.sha1()));
			Get frontGet2 = new Get(Bytes.toBytes("crawled-" + uh.sha1()));
			Result frontRow1 = frontTable.get(frontGet1);
			Result frontRow2 = frontTable.get(frontGet2);

			return frontRow1.isEmpty() || frontRow2.isEmpty();		
		}
	}

	public static class CrawlerReducer extends
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {
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
		public void reduce(ImmutableBytesWritable rowKey, Iterable<Text> crawlingURLs,
				Context context) throws IOException, InterruptedException {

			for (Text crawlingURL : crawlingURLs) {
				try {
					finishCrawling(rowKey.get(), crawlingURL.toString());
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

		}		

		private void finishCrawling(byte[] rowKey, String currentURL) throws NoSuchAlgorithmException, IOException{
			URLHelper uh = new URLHelper(currentURL);

			Delete frontDelete = new Delete(rowKey);
			frontDelete.deleteFamily(urlFamily);
			frontTable.delete(frontDelete);

			rowKey = Bytes.toBytes("crawled-" + uh.sha1());

			Put frontPut = new Put(rowKey);
	        frontPut.add(urlFamily, addressQual, Bytes.toBytes(currentURL));        
			frontTable.put(frontPut);		
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

		Scan scan = new Scan(Bytes.toBytes("t"));
		scan.addColumn(Bytes.toBytes(URL_COLUMN_FAMILY),
				Bytes.toBytes(ADDRESS_COLUMN_NAME));

		Job job = new Job(conf, "Retrieving seeds from frontier table ");

		job.setJarByClass(WebCrawler.class);
		TableMapReduceUtil.initTableMapperJob(FROINTER_TABLE_NAME, scan,
				CrawlerMapper.class, ImmutableBytesWritable.class, Writable.class, job);
		job.setReducerClass(CrawlerReducer.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Writable.class);
		FileOutputFormat.setOutputPath(job, new Path("out"));
		job.setNumReduceTasks(1);
				
		return job.waitForCompletion(true) ? 0 : 1;
	}
}