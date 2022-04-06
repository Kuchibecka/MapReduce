import MapReduce.MapReduceMain;
import MapReduce.Mapper;
import MapReduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MapReduce test
 *
 * Mapper test
 * Reducer test
 * Mapper + Reducer test
 */
public class MapReduceTest {
    // getting Log4j logger
    static Logger logger = Logger.getLogger(MapReduceMain.class);
    // mapDriver, reduceDriver, mapReduceDriver for simulating real Hadoop job processing
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    // making up assertion data: input data + expected result
    private final String map_test_string = "0, 54123658, 50";
    private final int map_answer_value = 50;
    private final String map_answer_text = "RAM, 54000000, h";

    @Before
    public void setUp() {
        System.setProperty("hadoop.home.dir", "/opt/hadoop-2.10.1");

        // configure drivers of Mapper, Reducer & combination of them (MapReduceDriver)
        Mapper mapper = new Mapper();
        Reducer reducer = new Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        // configure scale
        mapDriver.getConfiguration().set("scale", "h");
        mapReduceDriver.getConfiguration().set("scale", "h");

        // configure metrics references
        mapDriver.getConfiguration().set("metricsRef", "./metricsRef.txt");
        mapReduceDriver.getConfiguration().set("metricsRef", "./metricsRef.txt");
    }

    @Test
    public void testMap() {
        logger.debug("==============MAPPER TEST PROCESSING===================");
        try {
            mapDriver
                    .withInput(new LongWritable(), new Text(map_test_string))
                    .withOutput(new Text(map_answer_text), new IntWritable(map_answer_value))
                    .runTest();
        } catch (IOException e) {
            logger.error("===================ERROR OCCURRED====================");
            logger.error(e.getMessage());
            logger.error("=====================================================");
        }
        logger.debug("===============MAPPER TEST COMPLETE====================");
    }

    @Test
    public void testReduce() {
        logger.debug("==============REDUCE TEST PROCESSING===================");
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(100));
        values.add(new IntWritable(200));
        values.add(new IntWritable(60));
        values.add(new IntWritable(60));

        // avg(200, 100, 60, 60) = 105 (have to be this result)
        try {
            reduceDriver
                    .withInput(new Text(map_answer_text), values)
                    .withOutput(new Text(map_answer_text), new IntWritable(105))
                    .runTest();
        } catch (IOException e) {
            logger.error("===================ERROR OCCURRED====================");
            logger.error(e.getMessage());
            logger.error("=====================================================");
        }
        logger.debug("===============REDUCE TEST COMPLETE====================");
    }

    @Test
    public void testMapReduce() {
        logger.debug("=============MAPREDUCE TEST PROCESSING=================");
        try {
            mapReduceDriver
                    .withInput(new LongWritable(), new Text(map_test_string))
                    .withInput(new LongWritable(), new Text(map_test_string))
                    .withOutput(new Text(map_answer_text), new IntWritable((map_answer_value * 2) / 2))
                    .runTest();
        } catch (IOException e) {
            logger.error("===================ERROR OCCURRED====================");
            logger.error(e.getMessage());
            logger.error("=====================================================");
        }
        logger.debug("==============MAPREDUCE TEST COMPLETE==================");
    }
}
