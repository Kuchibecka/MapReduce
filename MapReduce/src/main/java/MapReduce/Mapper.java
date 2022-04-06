package MapReduce;

import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * Mapper class
 * Calculating scaled timestamp for Reducer
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
    // getting Log4j logger
    static Logger logger = Logger.getLogger(MapReduceMain.class);

    // metrics aggregation scale
    private static String scaleString;
    private static int scale;

    // metricId reference array
    private static ArrayList<String> metricsRef;

    /**
     * configuring mapreduce params
     * @param context - Hadoop mapreduce context
     */
    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();

        // getting metrics reference file
        String metricsRefPath = conf.get("metricsRef");
        Mapper.metricsRef = new ArrayList<>();

        // getting metrics references from metrics reference file
        try {
            File file = new File(metricsRefPath);
            FileReader fr = new FileReader(file);
            BufferedReader reader = new BufferedReader(fr);
            String line = reader.readLine();
            while (line != null) {
                Mapper.metricsRef.add(line);
                line = reader.readLine();
            }
            fr.close();
        } catch (IOException e) {
            logger.error("===================ERROR OCCURRED====================");
            logger.error(e.getMessage());
            logger.error("=====================================================");
        }

        // getting metricString nd interpreting it into number
        scaleString = conf.get("scale");
        switch (scaleString) {
            case "s":
                // second = 1000 millis
                scale = 1000;
                break;
            case "m":
                // minute = 60*1000 millis
                scale = 60000;
                break;
            case "h":
                // hour = 60*60*1000 millis
                scale = 3600000;
                break;
            case "d":
                // day = 24*60*60*1000 millis
                scale = 86400000;
                break;
            default:
                // millisecond
                scale = 1;
                break;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        logger.debug("===================MAP PROCESSING====================");
        IntWritable mapValue = new IntWritable();
        Text metricTimestamp = new Text();

        // calculating scaled timestamp
        String[] line = value.toString().split(", ");
        String timestampString = line[1];
        int timestamp = Integer.parseInt(timestampString);
        int scaledTimestamp = (timestamp / scale) * scale; // throwing away few digits from timestamp due to chosen scale

        // forming output key
        String metric = metricsRef.get(Integer.parseInt(line[0])); // get translated metric
        metricTimestamp.set(metric + ", " + scaledTimestamp + ", " + scaleString); // output text

        // assign value
        mapValue.set(Integer.parseInt(line[2]));

        try {
            // pushing out mapping result
            context.write(metricTimestamp, mapValue);
        } catch (InterruptedException | IOException e) {
            logger.error("===================ERROR OCCURRED====================");
            logger.error(e.getMessage());
            logger.error("=====================================================");
        }
        logger.debug("=======================MAP END=======================");
    }
}
