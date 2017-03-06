import static org.junit.Assert.fail;

import org.junit.Test;

import com.amazonaws.AmazonClientException;

import dexter.dynamoWrapper.DynamoDBHelper;

public class TestDBCreation {
	@Test
	public void test() {
		
		try{
			DynamoDBHelper.init();
			DynamoDBHelper.createTable("MUSIC", "singer", "title");
		
		}catch(AmazonClientException e){
			e.printStackTrace();
			fail ("Amazon client fail");
		}catch(Exception e){
			e.printStackTrace();
			fail("Not yet implemented");
		}
	}
}
