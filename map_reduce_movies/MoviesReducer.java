package MoviesNames;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MoviesReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
		int frequencyForCountry = 0;
		for (IntWritable val : values) {
        	frequencyForCountry += val.get();
     	}
		context.write(key, new IntWritable(frequencyForCountry));
	}
}
