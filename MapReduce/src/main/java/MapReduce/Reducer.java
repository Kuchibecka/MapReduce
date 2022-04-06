package MapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * Reducer class
 * Calculating average value for each key
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
    static Logger logger = Logger.getLogger(MapReduceMain.class);
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        logger.debug("=================REDUCE PROCESSING=====================");
        // calculating average metrics value
        int sum = 0, num = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            num += 1;
        }

        try {
            // pushing out reduce result
            context.write(key, new IntWritable(sum/num));
        } catch (IOException | InterruptedException e) {
            logger.error("===================ERROR OCCURRED====================");
            logger.error(e.getMessage());
            logger.error("=====================================================");
        }
        logger.debug("=====================REDUCE END========================");
    }
}
