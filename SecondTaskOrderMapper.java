/*
 * Author: Kaiqiang Huang
 * Student Number: D14122793
 * programming code: DT228/A
 * Stream: Data Analytics
 * */
package MapReduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/*This mapper is to sort for second task that is to count the average request by language*/
public class SecondTaskOrderMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	/*trim() is to remove spaces that are placed in the head and tail of string*/
    	/*split() is to split the string by space*/
        String[] split = value.toString().trim().split("\\s+");
        /*Output the name of project and the corresponding number of request count*/
        context.write(new DoubleWritable(Double.parseDouble(split[1])), new Text(split[0]));
    }
}
