package MoviesNames;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
// import java.util.StringTokenizer;

public class MoviesMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		String valueString = value.toString();
		String[] SingleCountryData = valueString.split("\t");
		context.write(new Text(SingleCountryData[8]), one);
	}
}
