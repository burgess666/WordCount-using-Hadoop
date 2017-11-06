/*
 * Author: Kaiqiang Huang
 * Student Number: D14122793
 * programming code: DT228/A
 * Stream: Data Analytics
 * */
package MapReduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/*The reducer is to output the results from sorting mapper*/
public class SecondTaskOrderReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{

    @Override
    /*Override the method of reduce to output results from the corresponding mapper*/
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    	/*for loop begins to output the name and result*/
        for (Text val: values) {
            context.write(val, key);
        }
        /*for loop ends*/
    }
}

