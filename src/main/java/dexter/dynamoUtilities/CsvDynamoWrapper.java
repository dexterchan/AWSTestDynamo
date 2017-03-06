package dexter.dynamoUtilities;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.opencsv.CSVReader;

import dexter.dynamoWrapper.DynamoDBHelper;

public class CsvDynamoWrapper {
	
	
	
	public void insertCsv2DynamoDB(String csvFile,String Table, String primaryKey, String... sortkey) throws Exception{
		
		String sortKeySet="";
		for ( int i = 0; i<sortkey.length;i++){
			sortKeySet = sortKeySet + sortkey[i];
		}
		if(sortkey.length==0){
			sortKeySet=null;
		}
		
		//Create Table
		DynamoDBHelper.init();
		DynamoDBHelper.createTable(Table, primaryKey, sortKeySet);
		
		CSVReader reader = null;
        try {
            reader = new CSVReader(new FileReader(csvFile));
            String[] line;
            ArrayList<String> fields=new ArrayList();
            
            //Read the first line
            line = reader.readNext();
            if(line!=null){
            	for (int i=0;i<line.length;i++)
            		fields.add(line[i]);
            }
            
            Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
            
            while ((line = reader.readNext()) != null) {
            	item.clear();
            	
            	for( int i=0;i<line.length;i++){
            		item.put(fields.get(i), new AttributeValue(line[i]));
            	}
            	DynamoDBHelper.insertDocument(Table, item);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
        	reader.close();
        }
	}
}
