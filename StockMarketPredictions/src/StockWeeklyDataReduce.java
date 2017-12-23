import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class StockWeeklyDataReduce extends Reducer<Text,Text,Text,Text> {

	/**
	 * @param args
	 */
	
	@Override
	public void reduce(Text weekNum, Iterable<Text> stockPrice, Context context){
		try {
			
			float sum = 0;
			int count = 0;
			
			//FloatWritable average;
			for (Text val : stockPrice) {
				sum = sum + Float.parseFloat(val.toString());
				count++;
			}
			Float avg = sum / count;
			Text average = new Text(avg.toString());
			
			context.write(weekNum, average);
			System.out.println("key in reduce:"+weekNum);
		} catch (Exception e) {
			System.out.println("Error in stockreduce:" + e.getMessage());

		}
		
	}
	

}
