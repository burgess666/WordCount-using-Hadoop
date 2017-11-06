/*
 * Author: Kaiqiang Huang
 * Student Number: D14122793
 * programming code: DT228/A
 * Stream: Data Analytics
 * */
package MapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/*The reducer is to output sorted results*/
public class FirstTaskOrderReducer extends Reducer<LongWritable, Text, Text, LongWritable>{
    @Override
    /*Output all result values and keys from Mapper*/
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    	
    	/*for loop begins*/
        for (Text value: values) {
            context.write(value, key);
        }
        /*for loop ends*/
    }
}
