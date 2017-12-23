import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.*;

public class StockMatrixMultReducer extends Reducer<Text, Text, Text, Text> {
	double a[][] = new double[3][3];
	double b[][] = new double[3][3];
	double c[][] = new double[3][3];
	Integer count = 0;

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		try {
			int x = 0;
			int y=0;
			for (Text val : value) {
				String s[]=val.toString().split("\t");
				String rows[] = s[1].toString().split(",");
				if(rows[0].equals("bull_bull"))
				{
					x=0;y=0;
				}
				else if(rows[0].equals("bull_bear"))
				{
					x=0;y=1;
				}
				else if(rows[0].equals("bull_stag"))
				{
					x=0;y=2;
				}
				else if(rows[0].equals("bear_bull"))
				{
					x=1;y=0;
				}
				else if(rows[0].equals("bear_bear"))
				{
					x=1;y=1;
				}
				else if(rows[0].equals("bear_stag"))
				{
					x=1;y=2;
				}
				else if(rows[0].equals("stag_bull"))
				{
					x=2;y=0;
				}
				else if(rows[0].equals("stag_bear"))
				{
					x=2;y=1;
				}
				else if(rows[0].equals("stag_stag"))
				{
					x=2;y=2;
				}
				
				Double f = Double.parseDouble(rows[1]);
				f = Math.rint(f * 1000);	//rounding to 3 decimal points
				a[x][y] = f / 1000;
				for (int i = 0; i < 3; i++) {
					for (int j = 0; j < 3; j++) {
						b[i][j] = a[i][j];
					}
				}
				count = 0;
				int l = 0;
				while (true) {
					multiply();
					if (equal()) {
						//count++;
						//System.out.println("count" + count);
						break;
					} else {
						//count++;
						System.out.println("in else"+"count:"+count);
						for (int i = 0; i < 3; i++) {
							for (int j = 0; j < 3; j++) {
								b[i][j] = c[i][j];

							}

						}
					}
					//l++;
					//System.out.println("l=" + l);
					count++;
				}
			}
			context.write(key, new Text(count.toString()));
			count = 0;

		} catch (Exception e) {
			System.out.println("Error in MatrixMulReduce:" + e.getMessage());
		}
	}

	//this function multiplies the two matrices
	void multiply() {
		double sum = 0;
		int i, j, k;
		for (i = 0; i < 3; i++) {
			for (j = 0; j < 3; j++) {
				for (k = 0; k < 3; k++) {
					sum = sum + a[i][k] * b[k][j];
				}
				c[i][j] = sum;
				sum = 0;
			}

		}
	}
	//compares the two matrices  upto 3 decimal points and returns true if they both are equal
	// and vice versa
	boolean equal() {
		boolean flag = true;
		// int count = 0;
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 3; j++) {
				double x = (c[i][j] - b[i][j]);
				x = Math.rint(x * 1000);
				if (!(x == 0)) {
					flag = false;
					break;
				}
				if (!flag) {
					break;
				}
			}
		}
		return flag;

	}
}
