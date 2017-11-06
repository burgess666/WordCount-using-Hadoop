/*
 * Author: Kaiqiang Huang
 * Student Number: D14122793
 * programming code: DT228/A
 * Stream: Data Analytics
 * */
package MapReduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*The mapper is used for first task that map the name of project and the number of request*/
public class FirstTaskMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	/*Fetch data from Wikipedia file and split them by space */
        String[] splits = value.toString().split("\\s+");
        /*Check the information that contains 4 characters or not*/
        if (splits.length == 4) {
        	/*Output first column and third column as snippet */
            context.write(new Text(splits[0]), new LongWritable(Long.parseLong(splits[2])));
        }
    }
}