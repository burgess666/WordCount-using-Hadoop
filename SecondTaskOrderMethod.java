/*
 * Author: Kaiqiang Huang
 * Student Number: D14122793
 * programming code: DT228/A
 * Stream: Data Analytics
 * */
package MapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/* The sorting method is for second mapper*/
public  class SecondTaskOrderMethod extends WritableComparator {
	protected SecondTaskOrderMethod() {
		super(IntWritable.class, true);
	}
    @Override
    /*Override the method of compare*/
    public int compare(WritableComparable value1, WritableComparable value2) {
        IntWritable key1 = (IntWritable) value1;
        IntWritable key2 = (IntWritable) value2;
        /*Compare all keys and time -1 to make a descending sorting  */
        int sort = (-1)*key1.compareTo(key2);
        /*Return the descending sorting*/
        return sort;
    }
}
