import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class StockStateTransitionReduce extends Reducer<Text, Text, Text, Text> {
	String currState;
	String prevState;
	
	public void reduce(Text key, Iterable<Text> status, Context context) {
		try {
			for (Text val : status) {
				System.out.println(val);
				String keys[]=key.toString().split("-");
				String rows[] = val.toString().split(",");
				if (rows[0].equalsIgnoreCase("curr")) {
					currState = rows[1];
				} else {
					prevState = rows[1];
				}
				
				context.write(new Text(keys[0]),new Text(prevState+'_'+currState));
			}
		} catch (Exception e) {
			System.out.println("Error in ProbReduce:"+e.getMessage());
		}

	}

}
