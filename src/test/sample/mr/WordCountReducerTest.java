package test.sample.mr;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import sample.mr.WordCount.WordCountReducer;

public class WordCountReducerTest {

    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    /**
     * 初期処理用メソッド
     */
    public void setup() {
        // Reducerオブジェクトの生成
        WordCountReducer reducer = new WordCountReducer();
        // テストドライバを生成
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        // rteduceメソッドへの入力を作成
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
