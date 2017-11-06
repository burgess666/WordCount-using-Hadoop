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
import org.apache.hadoop.mapreduce.Reducer;

/*The reducer is to count the number of request for first mapper*/
public class FirstTaskReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
    	
    	/*List all relevant values from Mapper as first result and save them to the file of first_snippet*/
        long requestCount = 0;
        /*for loop begins*/
        for (LongWritable value : values) {
        	/*sum the number of request*/
        	requestCount += value.get();
        }
        /*for loop ends*/
        /*Output the results*/
        context.write(key, new LongWritable(requestCount));
    }
}