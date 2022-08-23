
package wordFrequency;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopWordsReducer
	extends Reducer<DoubleWritable, WordWritable, WordWritable, DoubleWritable> {
    private int topWordPairsCount = 0;
    
    static final int TOP_WORD_PAIRS_COUNT_CAP = 100;
    
    @Override
    protected void reduce(DoubleWritable key,
    		Iterable<WordWritable> values, Context context
    		) throws IOException, InterruptedException {
    	for(WordWritable value : values) {
    		if(topWordPairsCount >= TOP_WORD_PAIRS_COUNT_CAP) {
    			break;
    		}
           
    		if(key.get() == 1.0) {
    			continue;
    		}
             
    		context.write(value, key);
    		topWordPairsCount++;
        }
    }
}
