import static org.junit.Assert.fail;

import java.util.HashMap;

import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import dexter.dynamoWrapper.DynamoDBHelper;

public class TestDBScan {

	@Test
	public void test() {
		try{
			DynamoDBHelper.init();
			
			String tableName="MUSIC";
			HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
            Condition condition = new Condition()
                .withComparisonOperator(ComparisonOperator.GT.toString())
                .withAttributeValueList(new AttributeValue().withN("2012"));
            Condition condition2 = new Condition()
                    .withComparisonOperator(ComparisonOperator.CONTAINS.toString())
                    .withAttributeValueList(new AttributeValue("Katy"));
            scanFilter.put("year", condition);
            scanFilter.put("singer", condition2);
			ScanResult r= DynamoDBHelper.scanDocument(tableName, scanFilter);
			System.out.println(r);
			
		}catch(AmazonClientException e){
			e.printStackTrace();
			fail ("Amazon client fail");
		}catch(Exception e){
			e.printStackTrace();
			fail("Not yet implemented");
		}
		
	}

}
