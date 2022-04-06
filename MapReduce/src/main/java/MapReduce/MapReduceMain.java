package MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Hadoop main class, configuring & running job
 */
public class MapReduceMain {
    // getting Log4j logger
    static Logger logger = Logger.getLogger(MapReduceMain.class);

    // mainFunction(inputDirectory, outputDirectory, aggregationScale)
    // aggregationScale: s - seconds, m - minutes, h - hours, d - days
    public static void main(String[] args) {
        BasicConfigurator.configure();

        if (args.length < 4) {
            throw new RuntimeException("Input args: input_dir, output_dir, scale (s,m,h,d)");
        }

        try {
            // getting input & output directories from cmd params
            Path inputDirectory = new Path(args[0]);
            Path outputDirectory = new Path(args[1]);

            // configuring scale & catalog
            Configuration conf = new Configuration();
            conf.set("scale", args[2]);
            conf.set("metricsRef", args[3]);

            // configuring hadoop job
            Job job = Job.getInstance(conf, "HADOOP JOB: Raw metrics aggregator");
            job.setJarByClass(MapReduceMain.class);
            job.setMapperClass(Mapper.class);
            job.setReducerClass(Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // c(8): number of reducers > 1
            job.setNumReduceTasks(2);

            // configure output format (sequence)
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // configuring input & output directories for Hadoop job
            FileInputFormat.addInputPath(job, inputDirectory);
            FileOutputFormat.setOutputPath(job, outputDirectory);

            logger.info("=====================JOB STARTED=====================");
            job.waitForCompletion(true);
            logger.info("======================JOB ENDED======================");
        } catch (IllegalArgumentException | IOException | IllegalStateException | InterruptedException | ClassNotFoundException e) {
            logger.error("===================ERROR OCCURRED====================");
            logger.error(e.getMessage());
            logger.error("=====================================================");
        }
    }
}
