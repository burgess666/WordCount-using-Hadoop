/*
 * Author: Kaiqiang Huang
 * Student Number: D14122793
 * programming code: DT228/A
 * Stream: Data Analytics
 * */
package MapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/*The mapper is for the second task that is to fetch the data of language and request number*/
public class SecondTaskMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
    public void map(LongWritable Key, Text value, Context context)
            throws IOException, InterruptedException {
        /*split string by space*/
        String[] split = value.toString().split("\\s+");
        /*check if string contains 2 characters*/
        if (split.length == 2) {
        	/*split first character by the symbol of dot*/
            String[] language = split[0].split("\\.");
            /*Output language and request count*/
            context.write(new Text(language[0]), new LongWritable(Long.parseLong(split[1])));
        }
    }
}

