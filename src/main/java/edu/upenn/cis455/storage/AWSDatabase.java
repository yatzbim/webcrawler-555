package edu.upenn.cis455.storage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class AWSDatabase {

    public static final String HTML_BUCKET = "cis555-g17-datastore";
    public static final String OUTURL_BUCKET = "g17outlinks";

    
    public static final String aws_access_key_id = "ASIAVOD6NBKXSITG2GG3";
    public static final String aws_secret_access_key = "lo/zAneHd0i41b6JyOVexeov1ldGzNJPI+krEuCc";
    public static final String aws_session_token = "FwoGZXIvYXdzEDsaDLO+6YYmuEfm/jyMxSLGAXpCgO89SPAbLedtBab3O48sD06Z/SgZtwtU0/piEIQBoZKyv2USTwf3/RwSW5GZlj0J6ytdBfXNPUYxj2HZriqlI4bpvCkuUAgo7kcszC1Lmi0wWNFcKLHDJbsp9Ny6QbbV5WqsqZCyuSOPG8zDZ4XjH8CrEgK9Tv4J//Ww77YpsnKsQGuV5lXZ92F7xJHoMY5vjAlW5nmUjgMnTLOQn5gaC/puVhW0hNshVV/W300YMY6lmFTammpZuBZY+5SQfL7xn4UdoCjx/rv1BTItTsUYeYqUavDXaSBBw4N6ov0+kRCN7sXz97KD1dU7x0ZOOfTY7CVesHZ4DI3s";
    static AWSCredentials credentials = new BasicSessionCredentials(aws_access_key_id, aws_secret_access_key, aws_session_token);

    // S3 client to access buckets
 //   static AmazonS3 s3Client = AmazonS3ClientBuilder
   //         .standard()
   //         .withCredentials(new AWSStaticCredentialsProvider(credentials))
   //         .withRegion(Regions.US_EAST_1)
   //         .build();
   //
   static AmazonS3 s3Client = AmazonS3ClientBuilder
            .standard()
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
//        System.out.println("HTML Success!");
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
//        System.out.println(url + ": Links Success!");
    }

//    public static void main(String[] args) {
//        s3Client.putObject(HTML_BUCKET, "http://crawltest.cis.upenn.edu", "hello!");
//        S3Object o = s3Client.getObject(HTML_BUCKET, "http://crawltest.cis.upenn.edu");
//        S3ObjectInputStream in = o.getObjectContent();
//        try {
//            int i = in.read();
//            while (i != -1) {
//                System.out.print((char) i);
//                i = in.read();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

}
