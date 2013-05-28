package test.sample.mr;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import sample.mr.WordCountNew.WordCountNewReducer;

public class WordCountNewReducerTest {
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

  @Before
  /**
   * 初期処理用メソッド
   */
  public void setUp() {
    // Reducerオブジェクトを生成
    WordCountNewReducer reducer = new WordCountNewReducer();
    // テストドライバを生成
    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
    reduceDriver.setReducer(reducer);
  }

  @Test
  public void testReducer() {
    // reduceメソッドへの入力を作成
    Text key = new Text("reduce");
    // valueリストを作成
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    reduceDriver.withInput(key, values);
    // reduceメソッドの出力結果の検証
    reduceDriver.withOutput(new Text("reduce"), new IntWritable(2));
    // テストの実行
    reduceDriver.runTest();
  }
}
