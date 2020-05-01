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

    public static final String HTML_BUCKET = "urlcontent";
    public static final String OUTURL_BUCKET = "outgoinglinks";

    
    public static final String aws_access_key_id = "ASIAZBMO5NPG3KBCJ576";
    public static final String aws_secret_access_key = "at0DwKAf6qquHgqALyaV7udjfU4dat3iVGCvLlpe";
    public static final String aws_session_token = "FwoGZXIvYXdzEAoaDO45PE88hfWIzyi2NiLGAT99Uq9IHowIa7ieOL69EpMbXPIMg5zStymzMQnQVtK5d2gWeIDqu63ln+kdufWWuwjuiWntsxK9TJe98p06pEMaiRzRE6c+vdYVYOL6WN4mcEi/+NV8v/FJUIqVRNpaN+Cp94j+1iFf5kJMR7+tT9Kad9xZCyiqHvy7iPWbhVFYgbGKIgXIziIqV0P95k4AdB4wc4gPRmr/h99hUcn/iRFOKDuF2O3PzPU9OANulGeY+xH6GG2wUWinjLF9qPLlPLiszqg/NSjxoLH1BTItU7EUX0cJnm9rrtpVjAAgNwW3qx2A+jwKeVoLfJX+etT9ny0m+fNvR1FhIIlb";
    static AWSCredentials credentials = new BasicSessionCredentials(aws_access_key_id,
            aws_secret_access_key, aws_session_token);

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
