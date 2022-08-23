
package wordFrequency;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LowerCaseMapper extends Mapper<LongWritable, Text, WordWritable, IntWritable> {
    private WordWritable word = new WordWritable();
    private IntWritable ONE = new IntWritable(1);
    private IntWritable wordFrequency = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context
    		) throws IOException, InterruptedException {
    	int relativeWordCount = 1;
        String[] wordStringArray = value.toString().split("\\s+");
        int wordStringArraySize = wordStringArray.length;
        if (wordStringArraySize > 1) {
            for (int i = 0; i < wordStringArraySize; i++) {
                if(wordStringArray[i].matches("^[A-Za-z]+$")) {
                    wordStringArray[i] = wordStringArray[i].replaceAll("\\W+", "");

                    if(wordStringArray[i].equals("")){
                        continue;
                    }

                    word.setCurrentWord(wordStringArray[i].toLowerCase());

                    /*
                     * Handle word frequency with start and end pointers.
                     */
                    int start = 0;
                    int end = i + relativeWordCount;
                    if(i > relativeWordCount){
                       start = i - relativeWordCount;
                    }
                    if(i + relativeWordCount >= wordStringArraySize){
                        end = wordStringArraySize - 1;
                    }

                    /*
                     * Write the next word with frequency being 1.
                     */
                    for (int j = start; j <= end; j++) {
                        if (j == i) {
                        	continue;
                        }
                        
                        if(wordStringArray[j].matches("^[A-Za-z]+$")) {
                          wordStringArray[j] = wordStringArray[j].replaceAll("\\W", "");
                          word.setNextWord(wordStringArray[j].toLowerCase());
                          context.write(word, ONE);
                        }
                    }

                    word.setNextWord("*");
                    
                    wordFrequency.set(end - start);
                    context.write(word, wordFrequency);
                }
            } // end of outer for loop
        }
    }
}
