
package wordFrequency;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class wordPairRelativeFrequencyMapper
       extends Mapper<Object, Text, DoubleWritable, WordWritable>{

       private String[] wordStringArray;
       private String[] wordTokens;
       private DoubleWritable relativeFrequency = new DoubleWritable();
  
       public void map(Object key, Text value, Context context
    		   ) throws IOException, InterruptedException {
    	   StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
           while (itr.hasMoreTokens()) {
        	   wordTokens = itr.nextToken().toString().split("\t");
        	   wordStringArray = wordTokens[0].toString().split(" ");
        	   WordWritable wordPair = 
        			   new WordWritable(wordStringArray[0], wordStringArray[1]);
        	   relativeFrequency.set(Double.parseDouble(wordTokens[1].trim()));
              
        	   if(relativeFrequency == null) {
        		   continue;
        	   }
              
        	   context.write(relativeFrequency, wordPair);
           }
       }
}
