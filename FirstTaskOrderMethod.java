/*
 * Author: Kaiqiang Huang
 * Student Number: D14122793
 * programming code: DT228/A
 * Stream: Data Analytics
 * */
package MapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*The method of order is to sort for first snippet*/
public class FirstTaskOrderMethod extends WritableComparator {
	
	/*Extend the class of WritableComparator*/
    protected FirstTaskOrderMethod() {
        super(LongWritable.class, true);
    }

    @Override
    public int compare(WritableComparable value1, WritableComparable value2) {
    	/*forcedly change the type of WritableComparable to the type of LongComparable*/
        LongWritable k1 = (LongWritable) value1;
        LongWritable k2 = (LongWritable) value2;
        int sort = (-1)*k1.compareTo(k2);
        /*change default order that is ascending to descending order*/
        return sort;
    }
}
