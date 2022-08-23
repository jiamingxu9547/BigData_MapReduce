
package wordFrequency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Date;

public class RelativeWordFrequencyTest {

    public static void main(String[] args
    		) throws IOException, InterruptedException, ClassNotFoundException {

    	/* Job 1 */
        Job job1 = Job.getInstance(new Configuration());
        job1.setJarByClass(RelativeWordFrequencyTest.class);
        job1.setJobName("RelativeWordFrequency");

        job1.setMapperClass(LowerCaseMapper.class);
        job1.setReducerClass(StarWordAndRelativeFrequencyCalculatorReducer.class);
        job1.setCombinerClass(WordCombinerReducer.class);
        job1.setPartitionerClass(WordPartitioner.class);
        job1.setNumReduceTasks(2);
        
        /*
         * Create the name of the output folder for being used as input for the next job.
         */
        String firstOutputFolder = "first-output-folder";
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(firstOutputFolder));

        job1.setOutputKeyClass(WordWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.waitForCompletion(true);

        /* Job 2 */
        Job job2 = Job.getInstance(new Configuration());
        job2.setJarByClass(RelativeWordFrequencyTest.class);
        job2.setJobName("RelativeWordFrequency2");
        job2.setMapperClass(wordPairRelativeFrequencyMapper.class);
        job2.setReducerClass(TopWordsReducer.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(WordWritable.class);
        job2.setSortComparatorClass(CustomKeyComparator.class);
        FileInputFormat.addInputPath(job2, new Path(firstOutputFolder));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
