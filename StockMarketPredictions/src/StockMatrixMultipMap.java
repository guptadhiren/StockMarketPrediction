import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import java.util.StringTokenizer;

public class StockMatrixMultipMap extends Mapper<Text,Text,Text,Text>{
		@ Override
		public void map (Text key, Text value, Context context){
			String compname="Null";
			try {
				String line= value.toString();
				StringTokenizer tokenizer= new StringTokenizer(line,"\t");
				if(tokenizer.hasMoreTokens())
				{
					compname=tokenizer.nextToken();
				}
				context.write(new Text(compname),value);
			} catch (Exception e) {
				System.out.println("Error in Matrix Multi : " + e.getMessage());
				System.out.println(e.getStackTrace());
			}
			
		}

	}

