package edu.utilities;

import org.apache.hadoop.hbase.util.Bytes;

public interface Constants {
	
	// Tables
	public static final String TABLE_REPOSITORY = "repository";
	public static final String TABLE_CRAWLED = "crawled";
	
	// Column Families
	public static final String COLUMNFAMILY_URL = "URL";
	public static final String COLUMNFAMILY_CONTENT = "content";
	public static final String COLUMNFAMILY_OUTGOING = "outgoing";
	public static final String COLUMNFAMILY_URLS="URLS";
	
	// Qualifiers 
	public static final String QUALIFIER_ADDRESS = "address";
	public static final String QUALIFIER_BODY = "body";
	public static final String QUALIFIER_LINKS = "links";
	
	// Columns  In bytes
	public static final byte[] COLUMNFAMILY_URL_BYTES = Bytes.toBytes(COLUMNFAMILY_URL);
	public static final byte[] COLUMNFAMILY_CONTENT_BYTES = Bytes.toBytes(COLUMNFAMILY_CONTENT);
	public static final byte[] COLUMNFAMILY_OUTGOING_BYTES = Bytes.toBytes(COLUMNFAMILY_OUTGOING);
	public static final byte[] COLUMNFAMILY_URLS_BYTES = Bytes.toBytes(COLUMNFAMILY_URLS);
	
	// Qualifiers  In bytes
	public static final byte[] QUALIFIER_ADDRESS_BYTES = Bytes.toBytes(QUALIFIER_ADDRESS);
	public static final byte[] QUALIFIER_BODY_BYTES = Bytes.toBytes(QUALIFIER_BODY);
	public static final byte[] QUALIFIER_LINKS_BYTES = Bytes.toBytes(QUALIFIER_LINKS);

	
	
		

}
