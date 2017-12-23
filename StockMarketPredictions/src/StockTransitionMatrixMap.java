import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class StockTransitionMatrixMap extends Mapper<Text,Text,Text,Text>{
	
	@Override
	public void map(Text key,Text value,Context context){
		
		try {
			context.write(key, value);
		} catch (Exception e) {
			System.out.println("error in Form transition Matrix:"+e.getMessage());
		}
	}
}
