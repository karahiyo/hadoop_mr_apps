package test.sample.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import sample.mr.WordCount.WordCountMapper;
import sample.mr.WordCount.WordCountReducer;

import java.io.IOException;

public class WordCountMapReduceTest {

    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>
        mapReduceDriver;


    /**
     * 初期処理用メソッド
     */
    @Before
        public void setUp() {
            // Mapperオブジェクトを生成
            WordCountMapper mapper = new WordCountMapper();
            // Reduceオブジェクトを生成
            WordCountReducer reducer = new WordCountReducer();
            // テストドライバを生成
            mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable,
                            Text, IntWritable>();
            mapReduceDriver.setMapper(mapper);
            mapReduceDriver.setReducer(reducer);
        }

    @Test
        public void testMapReduce() throws IOException {
            // mapメソッドへの入力
            mapReduceDriver.withInput(new LongWritable(0), new Text(
                        "hadoop hive pig map reduce map reduce hbase"));
            // reduceメソッドの出力結果の検証
            mapReduceDriver.withOutput(new Text("hadoop"), new IntWritable(1));
            mapReduceDriver.withOutput(new Text("hbase"), new IntWritable(1));
            mapReduceDriver.withOutput(new Text("hive"), new IntWritable(1));
            mapReduceDriver.withOutput(new Text("map"), new IntWritable(2));
            mapReduceDriver.withOutput(new Text("pig"), new IntWritable(1));
            mapReduceDriver.withOutput(new Text("reduce"), new IntWritable(2));
            mapReduceDriver.runTest();
        }
}



