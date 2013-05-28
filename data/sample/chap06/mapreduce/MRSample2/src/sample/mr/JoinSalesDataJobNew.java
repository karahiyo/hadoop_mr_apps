package sample.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 1段目のMapReduceジョブ
 * sales_detail内での合計金額の計算、およびsalesデータとの結合を行う
 */
public class JoinSalesDataJobNew {

  /**
   * SALESデータ識別用文字列
   */
  private static final String TYPE_SALES = "sales";

  /**
   * SALES_DETAILデータ識別用文字列
   */
  private static final String TYPE_SALES_DETAIL = "detail";

  /**
   * カラムセパレータ文字列
   */
  private static final String COLUMN_SEPARATOR = "\t";

  public static void main(String[] args) {

    try {
      // (1)Configurationオブジェクトの生成
      Configuration conf = new Configuration();
      // (2)汎用オプションの解析
      String[] otherArgs = new GenericOptionsParser(conf, args)
          .getRemainingArgs();
      if (otherArgs.length != 3) {
        System.err
            .println("Usage: sample.mr.JoinSalesDataJobNew <sales> <sales_detail> <out>");
        System.exit(2);
      }
      // (3)ジョブ起動用オブジェクトの生成
      Job job = new Job(conf, "JoinSalesDataJobNew");
      // (4)本クラスを含むjarファイルをジョブに登録(これによりスレーブノードのクラスパスにjarが組み込まれる)
      job.setJarByClass(JoinSalesDataJobNew.class);
      // (5)最終結果のKeyクラスを指定
      job.setOutputKeyClass(Text.class);
      // (6)最終結果のValueクラスを指定
      job.setOutputValueClass(Text.class);
      // (7)MultipleInputsによるsalesデータの入力指定
      MultipleInputs.addInputPath(job, new Path(args[0]),
          TextInputFormat.class, ForSalesDataMapper.class);
      // (8)MultipleInputsによるsales_detailデータの入力指定
      MultipleInputs.addInputPath(job, new Path(args[1]),
          TextInputFormat.class, ForSalesDetailDataMapper.class);
      // (9)Reducerクラスを指定
      job.setReducerClass(JoinSalesDataJobNewReducer.class);
      // (10)出力データ格納ディレクトリを指定
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      // (11)ジョブの実行を開始
      boolean result = job.waitForCompletion(true);
      System.exit(result ? 0 : 2);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * mapメソッドを実装した売上データ用Mapperクラス
   * ドライバクラス内に静的ネストクラスとして定義
   * 新APIではMapperクラスを拡張するのみ
   */
  public static class ForSalesDataMapper extends
      Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      // (12)valueの取得
      // 　　valueにテキストファイル1行分のデータが格納されている
      String line = value.toString();
      if (line == null || line.equals("")) {
        return;
      }

      line = line.trim();
      // (13)行テキストデータをスペース、タブ文字で分割
      String[] columns = line.split(COLUMN_SEPARATOR);
      String salesId = columns[0];
      String shopCode = columns[1];
      String salesDate = columns[2];
      // (14)Mapperから出力するkeyを設定
      outKey.set(salesId);
      // (15)Mapperから出力するvalueを設定
      outValue.set(TYPE_SALES + COLUMN_SEPARATOR + shopCode + COLUMN_SEPARATOR
          + salesDate);
      // (16)key/valueを出力
      context.write(outKey, outValue);
    }
  }

  /**
   * mapメソッドを実装した売上明細用Mapperクラス
   * ドライバクラス内に静的ネストクラスとして定義
   * 新APIではMapperクラスを拡張するのみ
   */
  public static class ForSalesDetailDataMapper extends
      Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      // (17)valueの取得
      // 　　valueにテキストファイル1行分のデータが格納されている
      String line = value.toString();
      if (line == null || line.equals("")) {
        return;
      }

      line = line.trim();
      // (18)行テキストデータをスペース、タブ文字で分割
      String[] columns = line.split(COLUMN_SEPARATOR);
      String salesId = columns[0];
      String itemCode = columns[2];
      int unitPrice = Integer.parseInt(columns[3]);
      int quantity = Integer.parseInt(columns[4]);
      // (19)Mapperから出力するkeyを設定
      outKey.set(salesId);
      // (20)Mapperから出力するvalueを設定
      outValue.set(TYPE_SALES_DETAIL + COLUMN_SEPARATOR + itemCode
          + COLUMN_SEPARATOR + unitPrice + COLUMN_SEPARATOR + quantity);
      // (21)key/valueを出力
      context.write(outKey, outValue);
    }
  }

  /**
   * reduceメソッドを実装したReducerクラス
   * ドライバクラス内に静的ネストクラスとして定義
   * 新APIではReducerクラスを拡張するのみ
   */
  public static class JoinSalesDataJobNewReducer extends
      Reducer<Text, Text, Text, Text> {

    Text outKey = new Text();
    Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // (22)Reducerに集約されたkeyを取得
      String salesId = key.toString();
      outKey.set(salesId);
      List<String> salesDetailList = new ArrayList<String>();
      String salesData = "";

      // (23)集約されたvalueのリストでループし、salesもしくはsales_detailに対する処理を行う
      Iterator<Text> ite = values.iterator();
      while (ite.hasNext()) {
        String value = ite.next().toString();
        String[] columns = value.split(COLUMN_SEPARATOR);

        // (24)SALES_DETAILデータの取り出し
        if (TYPE_SALES_DETAIL.equals(columns[0])) {
          salesDetailList.add(columns[1] + COLUMN_SEPARATOR + columns[2]
              + COLUMN_SEPARATOR + columns[3]);
          continue;
        }
        // (25)SALESデータの取り出し(1keyにつき1行しか現れない)
        if (TYPE_SALES.equals(columns[0])) {
          salesData = columns[1] + COLUMN_SEPARATOR + columns[2];
        }
      }
      // (26)SALESデータがReducerに渡ってきているかチェック
      if (salesData.equals("")) {
        return;
      }
      // (27)SALES_DETAILデータがReducerに渡ってきているかチェック
      if (salesDetailList.size() != 0) {
        for (String detailData : salesDetailList) {
          // (28)Reducerのvalueとして、SALESデータとSALES_DETAILデータの連結文字列をセット
          outValue.set(salesData + COLUMN_SEPARATOR + detailData);
          // (29)最終的なkeyとvalueを出力。このセットを1段目のMapReduceジョブの結果とする
          context.write(outKey, outValue);
        }
      }
    }
  }
}
