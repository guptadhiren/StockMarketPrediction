import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class StockTransitionMatrixReducer extends Reducer<Text, Text, Text, Text> {
	String pos;

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		try {
			int bull_bull = 0;
			int bull_bear = 0;
			int bull_stag = 0;
			int bear_bull=0;
			int bear_bear = 0;
			int bear_stag = 0;
			int stag_bull = 0;
			int stag_bear = 0;
			int stag_stag = 0;
	
			for (Text val : value) {
			
				if (val.toString().equals("bull_bull")) {
					bull_bull++;
					
				} else if (val.toString().equals("bull_bear")) {
					bull_bear++;
					
				} else if (val.toString().equals("bull_stag")) {
					bull_stag++;
					
				} else if (val.toString().equals("bear_bull")) {
					bear_bull++;
					
				} else if (val.toString().equals("bear_bear")) {
					bear_bear++;
					
				} else if (val.toString().equals("bear_stag")) {
					bear_stag++;
					
				} else if (val.toString().equals("stag_bull")) {
					stag_bull++;
					
				} else if (val.toString().equals("stag_bear")) {
					stag_bear++;
					
				} else if (val.toString().equals("stag_stag")) {
					stag_stag++;
					
				} else {
					throw new Exception("invalid input");
				}
			}
			context.write(key, new Text("bull_bull"+bull_bull/102));
			context.write(key, new Text("bull_bear"+bull_bear/102));
			context.write(key, new Text("bull_stag"+bull_stag/102));
			context.write(key, new Text("bear_bull"+bear_bull/102));
			context.write(key, new Text("bear_bear"+bear_bear/102));
			context.write(key, new Text("bear_stag"+bear_stag/102));
			context.write(key, new Text("stag_bull"+stag_bull/102));
			context.write(key, new Text("stag_bear"+stag_bear/102));
			context.write(key, new Text("stag_stag"+stag_stag/102));
		} catch (Exception e) {
			System.out.println("Error in formMatrixREduce:" + e.getMessage());
		}
	}
}
