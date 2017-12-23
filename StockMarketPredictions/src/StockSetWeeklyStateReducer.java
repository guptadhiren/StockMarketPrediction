import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class StockSetWeeklyStateReducer extends Reducer<Text,Text,Text,Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> value, Context context){
		double currWeekPrice = 0;
		double prevWeekPrice = 0;
		//Text strKey = new Text();
		Text strVal = new Text();
		try {
			for (Text val:value){
				String weekPrice[] = val.toString().split("-");
				if(weekPrice[0].equals("curr")){
					currWeekPrice = Double.parseDouble(weekPrice[1]);
				}else{
					prevWeekPrice = Double.parseDouble(weekPrice[1]);
				}
			}
			double threshold = 0.1 * prevWeekPrice;
			if((prevWeekPrice-threshold) < currWeekPrice &&  currWeekPrice < (prevWeekPrice+threshold))
				strVal.set("stag");
			if (currWeekPrice > (prevWeekPrice+threshold)){
				strVal.set("bull");
			}else if(currWeekPrice < prevWeekPrice-threshold){
				strVal.set("bear");
			}
			
			context.write(key, strVal);
		} catch (Exception e) {
			System.out.println("Error in Statereduce:" + e.getMessage());
		}
	}

}
