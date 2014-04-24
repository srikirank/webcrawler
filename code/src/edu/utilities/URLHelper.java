package edu.utilities;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class URLHelper {

	/**
	 * @param args
	 * @throws NoSuchAlgorithmException
	 */
	private URL url;

	public URL getUrl() {
		return url;
	}

	/*
	 * Sets URL using a given string. If not a valid URL, set to null
	 */
	public void setURL(String url) {
		try {
			this.url = new URL(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			this.url = null;
		}
	}

	public URLHelper(String url) {
		try {
			this.url = new URL(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			this.url = null;
		}
	}

	public URLHelper() {
	}

	/*
	 * Generates SHA from the URL object
	 */
	public String sha1() throws NoSuchAlgorithmException {
		MessageDigest mDigest = MessageDigest.getInstance("SHA1");
		byte[] result = mDigest.digest(url.toString().getBytes());
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < result.length; i++) {
			sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16)
					.substring(1));
		}
		return sb.toString();
	}

	/*
	 * The substring of SHA is used to form HBase row key
	 */
	private String subSha1() throws NoSuchAlgorithmException {
		return sha1().substring(0, 10);
	}

	private String getHost() {
		return url.getHost();
	}

	public String getReversedHost() {
		StringBuilder rHost = new StringBuilder();
		String[] parts = getHost().split("\\.");
		for (int i = parts.length - 1; i > 0; i--) {
			rHost.append(parts[i]).append(".");
		}
		rHost.append(parts[0]);
		return rHost.toString();
	}

	private StringBuilder getRHost() {
		StringBuilder rHost = new StringBuilder();
		String[] parts = getHost().split("\\.");
		for (int i = parts.length - 1; i > 0; i--) {
			rHost.append(parts[i]).append(".");
		}
		rHost.append(parts[0]);
		return rHost;
	}

	public String getDomain(){
		return getRHost().toString().split("\\.")[0];
	}
	
	public String generateKey() throws NoSuchAlgorithmException {
		StringBuilder key = getRHost();
		key.append("-").append(sha1());
		return key.toString();
	}

	public String generateTopDomain(){
		return null;
	}
	
	public static void main(String[] args) throws NoSuchAlgorithmException {
		URLHelper uh = new URLHelper();
		uh.setURL("http://www.sha1-online.com/sha1-java/");
		System.out.println(uh.sha1());
		uh.setURL("http://" + "sha1-online.com");
		System.out.println(uh.sha1());
		System.out.println(uh.subSha1());
		System.out.println(uh.getHost());
		System.out.println(uh.getReversedHost());
		System.out.println(uh.generateKey());
		System.out.println(uh.getDomain());
	}
}
