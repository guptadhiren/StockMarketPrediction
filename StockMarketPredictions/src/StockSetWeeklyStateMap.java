import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class StockSetWeeklyStateMap extends Mapper<Text,Text,Text,Text>{
	String stockKey;
	String stockVal;
	@Override
	public void map(Text key,Text value,Context context){
		try {
			String keys[] = key.toString().split("-");
			int week = Integer.parseInt(keys[1]);
			System.out.println(week);
			System.out.println(value);
			stockKey=keys[0]+"-"+(week-1)+","+week;
			System.out.println(stockKey);
			stockKey="curr-"+value;
			System.out.println("key,val"+stockKey+","+stockVal);
			context.write(new Text(stockKey),new Text(stockVal));
			stockKey = keys[0]+"-"+week+ ","+(week+1);
			stockVal = "prev-"+value;
			context.write(new Text(stockKey),new Text(stockVal));
		} catch (Exception e) {
			System.out.println("Error in setstate : " + e.getMessage());
			System.out.println(e.getStackTrace());
		}
	}
}
