package edu.upenn.cis455.storage;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class AWSDatabase {

//    public static final String HTML_BUCKET = "indexcontent";
//    public static final String OUTURL_BUCKET_1 = "worker-bucket-1";
//    public static final String OUTURL_BUCKET_2 = "worker-bucket-2";
    public static final String HTML_BUCKET = "tumbling-tumbleweeds";
    public static final String OUTURL_BUCKET_1 = "outgoinglinks-1";
    public static final String OUTURL_BUCKET_2 = "outgoinglinks-2";

    private static AtomicInteger whichLinkBucket = new AtomicInteger(1);
    
    public static final String aws_access_key_id = "ASIAZBMO5NPG2KYBIMO3";
    public static final String aws_secret_access_key = "2PohpbTL3SeBMWmdqE8XLQxG+MGwltaBMQkTvfjt";
    public static final String aws_session_token = "FwoGZXIvYXdzEJ7//////////wEaDJq/DmpbHH4RkzZSJSLGAbEGifaPhftUxOQE44byImO6GIpNos21iiGv457wuLR8EAhHeCh/po/Vh1sp/EbrU4rWLKPF0kh9un7pY9YvgbbW34pZlwv8uwGRtUZr0JayMxz1aOKiLYsOYdm1P/9ChsRSYeY5qNYnNIcEbI8BOvz0LsWAX8qnNrmqXXlI5N9iHt/PV4UTt9Utl3nctremt6WxqrB1LV6FE4qyWvlUpP15QMqxXbM69PYD3F1flfoshDT9ckqv3nb6+GJdXE+klkkVnutzICjl4tH1BTItJCN1AaGSVudVeGmY1Oj4FosJEh8IL8vwAuQEZEoNlZJfXTsFWKCkas93DJlx";
    public static final String input_directory = "input_directory";
    
    static AWSCredentials credentials = new BasicSessionCredentials("ASIA2RS2IRBE5FYW6FED", "KBigNejuDCdMMujEZg9s4TDfBvvh2iYKT52QDrym", "FwoGZXIvYXdzEJ7//////////wEaDIDHKgRlGmr1zQqQxSLEAaSZiCwqjKyjFPpKWE2y9KDuX2xTuw8NvNMR7Hu6Y4llSgTzXEmwqLYB8ozLJyLPkPjSJUVFC3wwr3iob2PeIGXBp3MPLQcOtpiubO/v1cfWmzp1STpmKsnQ/WSbRxfTFQV7C9NS0v6kZKVc6JM3t6cVUnRDzkTPXHj8preoHuN/9dqV1WDGf8GZZwvwyaSvJcpPAeagiOhWbfN8Gk+0ZL6PBWW/Gsc3MmXgZLrXUV3h1sFqAMSwOfW1h9N87RU6eF9oKoco6OrR9QUyLVAzkRHoai/w4YkPkWdS5J7EGRSTpY/ISbyfS40OJl7qpK7VmOk5Xaq6CAyEPA==");
    
    ObjectMetadata meta = new ObjectMetadata();
    
    // S3 client to access buckets
//    static AmazonS3 s3Client = AmazonS3ClientBuilder
//            .standard()
//            .withCredentials(new AWSStaticCredentialsProvider(credentials))
//            .withRegion(Regions.US_EAST_1)
//            .build();
   
//   static AmazonS3 s3Client = AmazonS3ClientBuilder
//            .standard()
//            .withRegion(Regions.US_EAST_1)
//            .build();
    
    static AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .withRegion(Regions.US_EAST_1)
            .build();

    MessageDigest digest;
    
    static AWSDatabase instance = null;
    
    public AWSDatabase() {
        try {
            digest = MessageDigest.getInstance("SHA-256");
            meta.setContentEncoding("UTF-8");
            s3Client.setBucketAcl(HTML_BUCKET, CannedAccessControlList.BucketOwnerFullControl);
            s3Client.setBucketAcl(OUTURL_BUCKET_1, CannedAccessControlList.BucketOwnerFullControl);
            s3Client.setBucketAcl(OUTURL_BUCKET_2, CannedAccessControlList.BucketOwnerFullControl);
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
    public void savePage(String url, String html) {
        if (url == null || html == null) {
            return;
        }
        
        InputStream input = new ByteArrayInputStream(html.getBytes());

        String key = RDS_Connection.encodeHex(digest.digest(url.getBytes()));

        s3Client.putObject(HTML_BUCKET, key, input, meta);
//        s3Client.putObject(HTML_BUCKET, key, html);
        s3Client.setObjectAcl(HTML_BUCKET, key, CannedAccessControlList.BucketOwnerFullControl);
    }

    // method to add a list of outgoing links
    public void saveOutgoingLinks(String url, List<String> urlList) {
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
        
        String key = RDS_Connection.encodeHex(digest.digest(url.getBytes()));
        
        if (whichLinkBucket.get() == 1) {
            s3Client.putObject(OUTURL_BUCKET_1, input_directory + "/" + key, urls);
            s3Client.setObjectAcl(OUTURL_BUCKET_1, input_directory + "/" + key, CannedAccessControlList.BucketOwnerFullControl);
            whichLinkBucket.incrementAndGet();
        } else if (whichLinkBucket.get() == 2) {
            s3Client.putObject(OUTURL_BUCKET_2, input_directory + "/" + key, urls);
            s3Client.setObjectAcl(OUTURL_BUCKET_2, input_directory + "/" + key, CannedAccessControlList.BucketOwnerFullControl);
            whichLinkBucket.decrementAndGet();
        } else {
            s3Client.putObject(OUTURL_BUCKET_2, input_directory + "/" + key, urls);
            s3Client.setObjectAcl(OUTURL_BUCKET_2, input_directory + "/" + key, CannedAccessControlList.BucketOwnerFullControl);
            whichLinkBucket.set(1);
        }
    }

    public static void main(String[] args) {
        
        String url = "https://en.wikipedia.org/wiki/Émile_Loubet";
        
        url = url.replace(" ", "%20");
        
        String key = "input_directory/" + RDS_Connection.digest("SHA-256", url);
        
//        String encodedString = null;
//        try {
//            encodedString = URLEncoder.encode(url, StandardCharsets.UTF_8.toString());
//        } catch (UnsupportedEncodingException e1) {
//            // TODO Auto-generated catch block
//            e1.printStackTrace();
//        }
//        
//        encodedString = encodedString.replace("%2F", "/");
//        encodedString = encodedString.replace("%3A", ":");
        
        System.out.println(url);
        
        System.out.println(key);
        S3Object o = s3Client.getObject(OUTURL_BUCKET_1, key);
        S3ObjectInputStream in = o.getObjectContent();
        
        InputStreamReader inR = new InputStreamReader(in, StandardCharsets.UTF_8);
        try {
            int i = inR.read();
            while (i != -1) {
                System.out.print((char) i);
                i = inR.read();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
