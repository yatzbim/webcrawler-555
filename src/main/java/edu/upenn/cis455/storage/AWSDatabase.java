package edu.upenn.cis455.storage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class AWSDatabase {

    public static final String HTML_BUCKET = "url-html";
    public static final String OUTURL_BUCKET = "url-outurls";

    static AWSCredentials credentials = new BasicAWSCredentials("AKIATVR2G2SUD7XGUFPG",
            "BibcYkzZmUktcmdLGLBWhejnqKkdJgPElKUzqBHa");

    // S3 client to access buckets
    static AmazonS3 s3Client = AmazonS3ClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.US_EAST_1)
            .build();

    MessageDigest digest;
    
    static AWSDatabase instance = null;

    
    public AWSDatabase() {
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    // singleton getInstance method
    public static synchronized AWSDatabase getInstance() {
        if (instance == null) {
            instance = new AWSDatabase();
        }
        return instance;
    }

    // method to add a new page to the buckets
    public synchronized void savePage(String url, String html) {
        if (url == null || html == null) {
            return;
        }

        String key = RDS_Connection.encodeHex(digest.digest(url.getBytes())); // TODO: test

        s3Client.putObject(HTML_BUCKET, key, html);
        System.out.println("HTML Success!");
    }

    // method to add a list of outgoing links
    public synchronized void saveOutgoingLinks(String url, List<String> urlList) {
        if (url == null || urlList == null) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < urlList.size(); i++) {
            sb.append(urlList.get(i));
            if (i != urlList.size() - 1) {
                sb.append('\n');
            }
        }

        String urls = sb.toString();
        
        String key = RDS_Connection.encodeHex(digest.digest(url.getBytes())); // TODO: Test

        s3Client.putObject(OUTURL_BUCKET, key, urls);
        System.out.println("Links Success!");
    }

    public static void main(String[] args) {
        s3Client.putObject(HTML_BUCKET, "http://crawltest.cis.upenn.edu", "hello!");
        S3Object o = s3Client.getObject(HTML_BUCKET, "http://crawltest.cis.upenn.edu");
        S3ObjectInputStream in = o.getObjectContent();
        try {
            int i = in.read();
            while (i != -1) {
                System.out.print((char) i);
                i = in.read();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
