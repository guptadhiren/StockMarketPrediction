import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class StockStateTransitionMap extends Mapper<Text,Text,Text,Text>{
	@ Override
	public void map (Text key, Text value, Context context){
		try {
			String keys[] = key.toString().split("-");
			System.out.println(keys[0]);
			System.out.println(keys[1]);
			String rows[] = keys[1].toString().split(",");
			Text key1 = new Text(keys[0]+":"+rows[0]);
			Text key2 = new Text (keys[0]+":"+rows[1]);
			Text state1 = new Text ("high,"+value);
			Text state2 = new Text ("low,"+value);
			context.write(key1,state1);
			context.write(key2, state2);
		} catch (Exception e) {
			System.out.println("Error in stateTransition : " + e.getMessage());
			System.out.println(e.getStackTrace());
		}
		
	}

}
