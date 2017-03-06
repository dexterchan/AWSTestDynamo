

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import dexter.dynamoWrapper.DynamoDBHelper;

public class TestDBInsert {
	@Test
	public void test() {
		try{
			DynamoDBHelper.init();
			
			String tableName="MUSIC";
			Map<String, AttributeValue> item = newItem("Katy Perry","Roar","Prism",2013);
			DynamoDBHelper.insertDocument(tableName, item);
			
			item = newItem("Katy Perry","Firework","Teenage Dream",2010);
			DynamoDBHelper.insertDocument(tableName, item);
			
			item = newItem("Taylor Swift","Blank Space","1989",2014);
			DynamoDBHelper.insertDocument(tableName, item);
			
			item = newItem("Taylor Swift","Red","Red",2012);
			DynamoDBHelper.insertDocument(tableName, item);
			
		}catch(AmazonClientException e){
			e.printStackTrace();
			fail ("Amazon client fail");
		}catch(Exception e){
			e.printStackTrace();
			fail("Not yet implemented");
		}
	}
	
	private static Map<String, AttributeValue> newItem(String singer,  String title,String album, int year) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("singer", new AttributeValue(singer));
        item.put("year", new AttributeValue().withN(Integer.toString(year)));
        item.put("title", new AttributeValue(title));
        item.put("album", new AttributeValue(album));
        //item.put("album", new AttributeValue().withSS(album));
        return item;
    }
}
