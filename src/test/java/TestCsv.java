import org.junit.Test;

import dexter.dynamoUtilities.CsvDynamoWrapper;

public class TestCsv {

	@Test
	public void test() {
		CsvDynamoWrapper csv = new CsvDynamoWrapper();
		
		try{
		csv.insertCsv2DynamoDB("/Users/dexter/Downloads/nyse/prices.csv","nyseprice", "symbol", "date");
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
