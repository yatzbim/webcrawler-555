package edu.upenn.cis455.storage;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class AWSDatabase {
    AWSCredentials credentials = new BasicAWSCredentials("AKIATVR2G2SUD7XGUFPG",
            "BibcYkzZmUktcmdLGLBWhejnqKkdJgPElKUzqBHa");

    // S3 client to access buckets
    AmazonS3 s3Client = AmazonS3ClientBuilder
            .standard().
            withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.US_EAST_1)
            .build();

    // dynamo client to access database
    AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.US_EAST_1)
            .build();
    DynamoDB db = new DynamoDB(dynamoClient);

    // Table object for robots.txt dynamo table
    Table table = db.getTable("HostPort-RobotsTxt");

}
