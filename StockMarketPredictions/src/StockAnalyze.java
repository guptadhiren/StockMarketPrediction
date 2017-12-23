import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

//import org.apache.hadoop
public class StockAnalyze {

	public static void findWeeklyPrice() throws IOException, ClassNotFoundException,
			InterruptedException {

		Configuration conf = new Configuration();

		Job j = new Job(conf, "StockAnalyze.class");

		Path pin = new Path("/home/dhiren/Desktop/input");
		Path pout = new Path("/home/dhiren/Desktop/output");

		j.setJarByClass(StockAnalyze.class);

		j.setMapperClass(StockWeeklyDataMap.class);
		j.setReducerClass(StockWeeklyDataReduce.class);

		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(Text.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);

		j.setInputFormatClass(TextInputFormat.class);
		j.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(j, pin);
		FileOutputFormat.setOutputPath(j, pout);

		j.waitForCompletion(true);
	}
	public static void findWeeklyStates() throws IOException, ClassNotFoundException,
	InterruptedException {
Configuration conf2 = new Configuration();

Job j2 = new Job(conf2, "StockAnalyze.class");

Path pin2 = new Path("/home/dhiren/Documents/hadoop/output");
Path pout2 = new Path("/home/dhiren/Documents/hadoop/output2");

j2.setJarByClass(StockAnalyze.class);

j2.setMapperClass(StockSetWeeklyStateMap.class);
j2.setReducerClass(StockSetWeeklyStateReducer.class);

j2.setMapOutputKeyClass(Text.class);
j2.setMapOutputValueClass(Text.class);
j2.setOutputKeyClass(Text.class);
j2.setOutputValueClass(Text.class);

j2.setInputFormatClass(SequenceFileInputFormat.class);
j2.setOutputFormatClass(SequenceFileOutputFormat.class);

FileInputFormat.addInputPath(j2, pin2);
FileOutputFormat.setOutputPath(j2, pout2);

j2.waitForCompletion(true);
}

public static void findTransitionStates() throws IOException, ClassNotFoundException,
	InterruptedException {
Configuration conf2 = new Configuration();

Job j2 = new Job(conf2, "StockAnalyze.class");

Path pin2 = new Path("/home/dhiren/Documents/hadoop/output2");
Path pout2 = new Path("/home/dhiren/Documents/hadoop/output3");

j2.setJarByClass(StockAnalyze.class);

j2.setMapperClass(StockStateTransitionMap.class);
j2.setReducerClass(StockStateTransitionReduce.class);

j2.setMapOutputKeyClass(Text.class);
j2.setMapOutputValueClass(Text.class);
j2.setOutputKeyClass(Text.class);
j2.setOutputValueClass(Text.class);

j2.setInputFormatClass(SequenceFileInputFormat.class);
j2.setOutputFormatClass(SequenceFileOutputFormat.class);

FileInputFormat.addInputPath(j2, pin2);
FileOutputFormat.setOutputPath(j2, pout2);

j2.waitForCompletion(true);
}

public static void createTransitionMatrix() throws IOException, ClassNotFoundException,
	InterruptedException {

Configuration conf = new Configuration();

Job j = new Job(conf, "StockAnalyze.class");

Path pin = new Path("/home/dhiren/Documents/hadoop/output3");
Path pout = new Path("/home/dhiren/Documents/hadoop/output4");

j.setJarByClass(StockAnalyze.class);

j.setMapperClass(StockTransitionMatrixMap.class);
j.setReducerClass(StockTransitionMatrixReducer.class);

j.setMapOutputKeyClass(Text.class);
j.setMapOutputValueClass(Text.class);
j.setOutputKeyClass(Text.class);
j.setOutputValueClass(Text.class);

j.setInputFormatClass(SequenceFileInputFormat.class);
j.setOutputFormatClass(SequenceFileOutputFormat.class);

FileInputFormat.addInputPath(j, pin);
FileOutputFormat.setOutputPath(j, pout);

j.waitForCompletion(true);
}

public static void MatrixMulti() throws IOException, ClassNotFoundException,
	InterruptedException {

Configuration conf = new Configuration();

Job j = new Job(conf, "StockAnalyze.class");

Path pin = new Path("/home/dhiren/Documents/hadoop/output4");
Path pout = new Path("/home/dhiren/Documents/hadoop/output5");

j.setJarByClass(StockAnalyze.class);

j.setMapperClass(StockMatrixMultipMap.class);
j.setReducerClass(StockMatrixMultReducer.class);

j.setMapOutputKeyClass(Text.class);
j.setMapOutputValueClass(Text.class);
j.setOutputKeyClass(Text.class);
j.setOutputValueClass(Text.class);

j.setInputFormatClass(SequenceFileInputFormat.class);
j.setOutputFormatClass(TextOutputFormat.class);

FileInputFormat.addInputPath(j, pin);
FileOutputFormat.setOutputPath(j, pout);

j.waitForCompletion(true);
}

		public static void main(String args[]) throws IOException,
			ClassNotFoundException, NoClassDefFoundError, InterruptedException {
			findWeeklyPrice();
			findWeeklyStates();
			findTransitionStates();
			createTransitionMatrix();
			MatrixMulti();
			
		}

}
