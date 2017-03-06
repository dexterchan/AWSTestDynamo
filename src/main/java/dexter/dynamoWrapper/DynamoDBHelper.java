package dexter.dynamoWrapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

public class DynamoDBHelper {
	static AmazonDynamoDBClient dynamoDB;
	static boolean initialized=false;
	
	private static final Logger log = LogManager.getLogger(DynamoDBHelper.class);
	
	public static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (/Users/dexter/.aws/credentials).
         */
		if(initialized){
			synchronized (DynamoDBHelper.class){
				if(initialized){
					//No need to do anything
					return;
				}
			}
		}
		
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/Users/dexter/.aws/credentials), and is in valid format.",
                    e);
        }
        dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        dynamoDB.setRegion(usEast1);
        initialized=true;
    }
	
	public static void createTable(String tableName,String primarykey, String sortkey) throws Exception{
		// Create a table with a primary hash key named 'name', which holds a string
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
            .withKeySchema(new KeySchemaElement().withAttributeName(primarykey).withKeyType(KeyType.HASH))
            .withAttributeDefinitions(new AttributeDefinition().withAttributeName(primarykey).withAttributeType(ScalarAttributeType.S))
            .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
        
        if(sortkey!=null && sortkey.length()>0){
        	createTableRequest.withKeySchema(new KeySchemaElement().withAttributeName(sortkey).withKeyType(KeyType.RANGE))
        	.withAttributeDefinitions(new AttributeDefinition().withAttributeName(sortkey).withAttributeType(ScalarAttributeType.S));
        }
        
        log.info("create table "+tableName+" start");
        TableUtils.createTableIfNotExists(dynamoDB, createTableRequest);
        log.info("create table "+tableName+" end");
        TableUtils.waitUntilActive(dynamoDB, tableName);
		
	}
	
	public static PutItemResult insertDocument(String tableName,Map<String, AttributeValue> item){
		PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
        
        return putItemResult;
	}
	
	public static ScanResult scanDocument(String tableName,  HashMap<String, Condition> scanFilter){
		
        ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
        ScanResult scanResult = dynamoDB.scan(scanRequest);
        
        return scanResult;
	}
	
}
