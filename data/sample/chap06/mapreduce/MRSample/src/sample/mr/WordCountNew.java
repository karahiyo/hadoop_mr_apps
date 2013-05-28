package sample.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ワードカウントMapReduceジョブ(新API)
 */
public class WordCountNew {

  public static void main(String[] args) {

    try {
      
      // (1)Hadoop設定オブジェクトの生成
      Configuration conf = new Configuration();
      // (2)ジョブ起動用オブジェクトの生成
      Job job = new Job(conf, "WordCountNew");
      // (3)本クラスを含むjarファイルをジョブに登録
      //   (これによりスレーブノードのクラスパスにjarが組み込まれる)
      job.setJarByClass(WordCountNew.class);
      // (4)最終結果のKeyクラスを指定
      job.setOutputKeyClass(Text.class);
      // (5)最終結果のValueクラスを指定
      job.setOutputValueClass(IntWritable.class);
      // (6)Mapperクラスを指定
      job.setMapperClass(WordCountNewMapper.class);
      // (7)Reducerクラスを指定
      job.setReducerClass(WordCountNewReducer.class);
      // (8)入力データ格納ディレクトリを指定
      FileInputFormat.addInputPath(job, new Path(args[0]));
      // (9)出力データ格納ディレクトリを指定
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      // (10)ジョブの実行を開始
      boolean result = job.waitForCompletion(true);
      System.exit(result ? 0 : 2);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * mapメソッドを実装したMapperクラス
   * ドライバクラス内に静的ネストクラスとして定義
   * 新APIではMapperクラスを拡張する
   */
  public static class WordCountNewMapper extends
      Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      // (11)valueの取得
      // 　　valueにテキストファイル1行分のデータが格納されている
      String line = value.toString();
      if (line == null || line.equals("")) {
        return;
      }
      line = line.trim();
      // (12)行テキストデータをスペース、タブ文字で分割
      String[] words = line.split("\\s");
      for (String word : words) {
        // (13)すべて小文字にし、mapメソッドの出力キーとする
        outKey.set(word.toLowerCase());
        // (14)valueは1のため、フィールドで定義したoutValueをそのまま出力
        context.write(outKey, outValue);
      }
    }
  }

  /**
   * reduceメソッドを実装したReducerクラス 
   * ドライバクラス内に静的ネストクラスとして定義 
   * 新APIではReducerクラスを拡張する
   */
  public static class WordCountNewReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {

    Text outKey = new Text();
    IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      // (15)Reducerに集約されたkeyを取得
      outKey.set(key);
      int count = 0;
      // (16)集約されたvalueのリストでループし、valueとして渡されたカウントを足し合わせる
      Iterator<IntWritable> ite = values.iterator();
      while (ite.hasNext()) {
        int c = ite.next().get();
        count += c;
      }
      // (17)Reducerのvalueとして、合計したカウント値をセット
      outValue.set(count);
      // (18)最終的なkeyとvalueを出力。このセットが最終結果としてファイルに書き込まれる
      context.write(outKey, outValue);
    }
  }
}
