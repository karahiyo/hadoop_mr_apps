package test.sample.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import sample.mr.WordCount.WordCountMapper;

import java.io.IOException;

public class WordCountMapperTest {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
        /**
         * 初期化処理用メソッド
         */
        public void setup() {
            // Mapperオブジェクトを生成
            WordCountMapper mapper = new WordCountMapper();
            // テストドライバ生成
            mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
            mapDriver.setMapper(mapper);
        }

    @Test
        /**
         * Mapperテストケース
         */
        public void testMapper() throws IOException {
            // mapメソッドへの入力
            mapDriver.withInput(new LongWritable(0), new Text(
                        "hadoop hive pig map reduce map reduce hbase"));
            // mapメソッドの出力結果の検証
            mapDriver.withOutput(new Text("hadoop"), new IntWritable(1));
            mapDriver.withOutput(new Text("hive"), new IntWritable(1));
            mapDriver.withOutput(new Text("pig"), new IntWritable(1));
            mapDriver.withOutput(new Text("map"), new IntWritable(1));
            mapDriver.withOutput(new Text("reduce"), new IntWritable(1));
            mapDriver.withOutput(new Text("map"), new IntWritable(1));
            mapDriver.withOutput(new Text("reduce"), new IntWritable(1));
            mapDriver.withOutput(new Text("hbase"), new IntWritable(1));
            mapDriver.runTest();
        }
}
