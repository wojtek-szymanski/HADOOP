package pl.com.sages.hadoop.mapreduce.hadoopinaction;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class InvertedListMapReduceTest {

    MapDriver<Text, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<Text, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        InvertedListMapper mapper = new InvertedListMapper();
        InvertedListReducer reducer = new InvertedListReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new Text("from"), new Text("to"));
        mapDriver.withOutput(new Text("to"), new Text("from"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("from"), Lists.newArrayList(new Text("a"), new Text("b"), new Text("c")));
        reduceDriver.withOutput(new Text("from"), new Text("a,b,c"));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new Text("1"), new Text("a"));
        mapReduceDriver.withInput(new Text("2"), new Text("a"));
        mapReduceDriver.withOutput(new Text("a"), new Text("1,2"));
        mapReduceDriver.runTest();
    }
}