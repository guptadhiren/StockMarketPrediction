
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class StockWeeklyDataMap extends Mapper<LongWritable,Text,Text,Text> {

	/**
	 * @param args
	 */
	Text weekNum;
	Text stockPrice;
	@Override
	public void map(LongWritable key, Text value,Context context){
		try {
			String rows[] = value.toString().split(",");
			SimpleDateFormat Sdf = new SimpleDateFormat("yyyy/MM/dd");
			Date date = Sdf.parse(rows[0]);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			Integer week = cal.get(Calendar.WEEK_OF_YEAR);
			
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			String s = fileName + "-" +week.toString();
			weekNum = new Text(s);
			stockPrice = new Text(rows[1]);
			context.write(weekNum, stockPrice);
			System.out.println("key in map:"+weekNum);
		} catch (Exception e) {
			System.out.println("Error in stockmaper : " + e.getMessage());
		}
	}

}
