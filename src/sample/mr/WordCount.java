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
 *  ワードカウントMapReduceジョブ 
 *  using org.apache.hadoop.mapreduce new API.
 */

public class WordCount {

    public static void main(String[] args) {

        try {
            // Hadoop設定オブジェクトの生成
            Configuration conf = new Configuration();
            // ジョブ起動用オブジェクトの生成
            Job job = new Job(conf, "WordCount");
            // 本クラスを含むjarファイルをジョブに登録
            // (これにより、スレーブノードのクラスパスにjarが組み込まれる)
            job.setJarByClass(WordCount.class);
            // 最終結果のKeyクラスを指定
            job.setOutputKeyClass(Text.class);
            // 最終結果のValueクラスを指定
            job.setOutputValueClass(IntWritable.class);
            // Mapperクラスを指定
            job.setMapperClass(WordCountMapper.class);
            // Reducerクラスを指定
            job.setReducerClass(WordCountReducer.class);
            // 入力データ格納ディレクトリ指定
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // 出力データ格納ディレクトリ指定
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // ジョブの実行を開始
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * mapメソッドを実装したMapperクラス
     * ドライバクラス内に静的ネストクラスとして定義
     * 新APIでは、Mapperクラスを拡張する
     */
    public static class WordCountMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {

            private Text outKey = new Text();
            private IntWritable outValue = new IntWritable(1);
            
            @Override
            protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

                // Valueの取得
                String line = value.toString();
                if (line == null || line.equals("")) {
                    return;
                }
                line = line.trim();
                // 行テキストデータをスペース、タブで分割
                String[] words = line.split("\\s");
                for (String word : words) {
                    // すべて小文字にし、mapメソッドの出力キーとする
                    outKey.set(word.toLowerCase());
                    // valueは1のため、フィールドで定義したoutValueをそのまま出力
                    context.write(outKey, outValue);
                }
            }
        }


    /**
     * reduceメソッドを実装したReducerクラス
     * ドライバクラス内に静的ネストクラスとして定義
     * 新APIではReducerクラスを拡張する
     */
    public static class WordCountReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {

            Text outKey = new Text();
            IntWritable outValue = new IntWritable();

            @Override
            protected void reduce(Text key, Iterable<IntWritable> values,
                    Context context) throws IOException, InterruptedException {
                // Reducerに集約されたKeyを取得
                outKey.set(key);
                int count = 0;
                // 集約されたvalueのリストでループし、valueとして渡されたカウントを足し合わせる
                Iterator<IntWritable> ite = values.iterator();
                while(ite.hasNext()) {
                    int c = ite.next().get();
                    count += c;
                }
                // Reducerのvalueとして、合計したカウント値をセット
                outValue.set(count);
                // 最終的なkryとvalueを出力。このセットが最終的な結果をしてファイルに書き出される
                context.write(outKey, outValue);
            }
        }
}









