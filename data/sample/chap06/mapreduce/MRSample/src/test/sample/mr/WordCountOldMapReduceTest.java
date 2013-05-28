package test.sample.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import sample.mr.WordCountOld.WordCountOldMapper;
import sample.mr.WordCountOld.WordCountOldReducer;


public class WordCountOldMapReduceTest {
  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  /**
   * 初期処理用メソッド
   */
  @Before
  public void setUp() {
    // Mapperオブジェクトを生成
    WordCountOldMapper mapper = new WordCountOldMapper();
    // Reducerオブジェクトを生成
    WordCountOldReducer reducer = new WordCountOldReducer();
    // テストドライバを生成
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
  }

  @Test
  public void testMapReduce() {
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
    // テストの実行
    mapReduceDriver.runTest();
  }
}
