
package wordFrequency;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCombinerReducer 
	extends Reducer<WordWritable, IntWritable, WordWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(WordWritable key, Iterable<IntWritable> values, Context context
    		) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
             count += value.get();
        }
        result.set(count);
        context.write(key, result);
    }
}
