
package wordFrequency;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StarWordAndRelativeFrequencyCalculatorReducer
	extends Reducer<WordWritable, IntWritable, WordWritable, DoubleWritable> {
	private DoubleWritable relativeStarWordCount = new DoubleWritable();
	private DoubleWritable totalStarWordCount = new DoubleWritable();
    private Text currentWord = new Text("NO_WORD_SET_YET");
    private Text flag = new Text("*");

    @Override
    protected void reduce(WordWritable key, Iterable<IntWritable> values, Context context
    		) throws IOException, InterruptedException {
        if (key.getNextWord().equals(flag)) {
            if (key.getCurrentWord().equals(currentWord)) {
            	totalStarWordCount.set(totalStarWordCount.get() + obtainIntSum(values));
            }else{
                currentWord.set(key.getCurrentWord());
                totalStarWordCount.set(0);
                totalStarWordCount.set(obtainIntSum(values));
            }
        }else{
        	int count = obtainIntSum(values);
            relativeStarWordCount.set((double) count / totalStarWordCount.get());
            context.write(key, relativeStarWordCount);
        }

    }

    private int obtainIntSum(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable value : values) {
        	sum += value.get();
        }
        return sum;
    }
}
