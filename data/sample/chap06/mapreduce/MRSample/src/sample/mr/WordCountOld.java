package sample.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * ワードカウントMapReduceジョブ(旧API)
 */
public class WordCountOld {

  public static void main(String[] args) {

    int result = 0;
    try {
      // (1)ジョブ起動用オブジェクトの生成
      JobConf conf = new JobConf(new Configuration(), WordCountOld.class);
      // (2)最終結果のKeyクラスを指定
      conf.setOutputKeyClass(Text.class);
      // (3)最終結果のValueクラスを指定
      conf.setOutputValueClass(IntWritable.class);
      // (4)Mapperクラスを指定
      conf.setMapperClass(WordCountOldMapper.class);
      // (5)Reducerクラスを指定
      conf.setReducerClass(WordCountOldReducer.class);
      // (6)入力データ格納ディレクトリを指定
      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      // (7)出力データ格納ディレクトリを指定
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
      // (8)ジョブの実行を開始
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
      result = 2;
    }
    System.exit(result);
  }

  /**
   * mapメソッドを実装したMapperクラス
   * ドライバクラス内に静的ネストクラスとして定義
   * 1行分のデータに対して単語分割を行う
   */
  public static class WordCountOldMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> collector, Reporter reporter)
        throws IOException {

      // (9)valueの取得
      // 　　valueにテキストファイル1行分のデータが格納されている
      String line = value.toString();
      if (line == null || line.equals("")) {
        return;
      }
      line = line.trim();
      // (10)行テキストデータをスペース、タブ文字で分割
      String[] words = line.split("\\s");
      for (String word : words) {
        // (11)すべて小文字にし、mapメソッドの出力キーとする
        outKey.set(word.toLowerCase());
        // (12)値は1のため、フィールドで定義したoutValueをそのまま出力
        collector.collect(outKey, outValue);
      }
    }
  }

  /**
   * reduceメソッドを実装したReducerクラス 
   * ドライバクラス内に静的ネストクラスとして定義
   */
  public static class WordCountOldReducer extends MapReduceBase implements
      Reducer<Text, IntWritable, Text, IntWritable> {

    Text outKey = new Text();
    IntWritable outValue = new IntWritable();

    @Override
    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> collector, Reporter reporter)
        throws IOException {
      // (13)Reducerに集約されたkeyを取得
      outKey.set(key);
      int count = 0;
      // (14)集約されたvalueのリストでループし、valueとして渡されたカウントを足し合わせる
      while (values.hasNext()) {
        int c = values.next().get();
        count += c;
      }
      // (15)Reducerのvalueとして、合計したカウント値をセット
      outValue.set(count);
      // (16)最終的なkeyとvalueを出力。このセットが最終結果としてファイルに書き込まれる
      collector.collect(outKey, outValue);
    }
  }
}
