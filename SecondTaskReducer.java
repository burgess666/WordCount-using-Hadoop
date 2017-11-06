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
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/*The reducer is to output the average number of request during 9 hours*/
public class SecondTaskReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long count = 0;
        /*manually set up the number of selected period that is equal to the number of involved files */
        double period = 9;
        /*for loop begins to sum request number*/
        for (LongWritable value : values) {
            count += value.get();
        }
        /*for loop end*/
        /*count average number of request by hours*/
        double avg = count / period;
        context.write(key, new DoubleWritable(avg));
    }
}
