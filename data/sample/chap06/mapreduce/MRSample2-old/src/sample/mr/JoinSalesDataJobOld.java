package sample.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 1段目のMapReduceジョブ
 * sales_detail内での合計金額の計算、およびsalesデータとの結合を行う
 */
public class JoinSalesDataJobOld {

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
    int result = 0;
    try {
      // (1)Configurationオブジェクトの生成
      Configuration conf = new Configuration();
      // (2)汎用オプションの解析
      String[] otherArgs = new GenericOptionsParser(conf, args)
          .getRemainingArgs();
      if (otherArgs.length != 3) {
        System.err
            .println("Usage: sample.mr.JoinSalesDataJobOld <sales> <sales_detail> <out>");
        System.exit(2);
      }
      // (3)ジョブ起動用オブジェクトの生成
      JobConf job = new JobConf(conf, JoinSalesDataJobOld.class);
      // (4)最終結果のKeyクラスを指定
      job.setOutputKeyClass(Text.class);
      // (5)最終結果のValueクラスを指定
      job.setOutputValueClass(Text.class);
      // (6)MultipleInputsによるsalesデータの入力指定
      MultipleInputs.addInputPath(job, new Path(args[0]),
          TextInputFormat.class, ForSalesDataOldMapper.class);
      // (7)MultipleInputsによるsales_detailデータの入力指定
      MultipleInputs.addInputPath(job, new Path(args[1]),
          TextInputFormat.class, ForSalesDetailDataOldMapper.class);
      // (8)Reducerクラスを指定
      job.setReducerClass(JoinSalesDataJobOldReducer.class);
      // (9)出力データ格納ディレクトリを指定
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      // (10)ジョブの実行を開始
      JobClient.runJob(job);

    } catch (Exception e) {
      e.printStackTrace();
      result = 2;
    }
    System.exit(result);
  }

  /**
   * mapメソッドを実装した売上データ用Mapperクラス 
   * ドライバクラス内に静的ネストクラスとして定義 
   */
  public static class ForSalesDataOldMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, Text> {
    
    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> collector, Reporter reporter)
        throws IOException {

      // (11)valueの取得
      // 　　valueにテキストファイル1行分のデータが格納されている
      String line = value.toString();
      if (line == null || line.equals("")) {
        return;
      }

      line = line.trim();
      // (12)行テキストデータをスペース、タブ文字で分割
      String[] columns = line.split(COLUMN_SEPARATOR);
      String salesId = columns[0];
      String shopCode = columns[1];
      String salesDate = columns[2];
      // (13)Mapperから出力するkeyを設定
      outKey.set(salesId);
      // (14)Mapperから出力するvalueを設定
      outValue.set(TYPE_SALES + COLUMN_SEPARATOR + shopCode + COLUMN_SEPARATOR
          + salesDate);
      // (15)key/valueを出力
      collector.collect(outKey, outValue);
    }
  }

  /**
   * mapメソッドを実装した売上明細データ用Mapperクラス
   * ドライバクラス内に静的ネストクラスとして定義
   */
  public static class ForSalesDetailDataOldMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, Text> {
    
    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> collector, Reporter reporter)
        throws IOException {

      // (16)valueの取得
      // 　　valueにテキストファイル1行分のデータが格納されている
      String line = value.toString();
      if (line == null || line.equals("")) {
        return;
      }

      line = line.trim();
      // (17)行テキストデータをスペース、タブ文字で分割
      String[] columns = line.split(COLUMN_SEPARATOR);
      String salesId = columns[0];
      String itemCode = columns[2];
      int unitPrice = Integer.parseInt(columns[3]);
      int quantity = Integer.parseInt(columns[4]);
      // (18)Mapperから出力するkeyを設定
      outKey.set(salesId);
      // (19)Mapperから出力するvalueを設定
      outValue.set(TYPE_SALES_DETAIL + COLUMN_SEPARATOR + itemCode
          + COLUMN_SEPARATOR + unitPrice + COLUMN_SEPARATOR + quantity);
      // (20)key/valueを出力
      collector.collect(outKey, outValue);
    }
  }

  /**
   * reduceメソッドを実装したReducerクラス 
   * ドライバクラス内に静的ネストクラスとして定義 
   */
  public static class JoinSalesDataJobOldReducer extends MapReduceBase implements
      Reducer<Text, Text, Text, Text> {
    Text outKey = new Text();
    Text outValue = new Text();

    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> collector, Reporter reporter)
        throws IOException {
      // (21)Reducerに集約されたkeyを取得
      String salesId = key.toString();
      outKey.set(salesId);
      List<String> salesDetailList = new ArrayList<String>();
      String salesData = "";

      // (22)集約されたvalueのリストでループし、salesもしくはsales_detailに対する処理を行う
      while (values.hasNext()) {
        String value = values.next().toString();
        String[] columns = value.split(COLUMN_SEPARATOR);

        // (23)SALES_DETAILデータの取り出し
        if (TYPE_SALES_DETAIL.equals(columns[0])) {
          salesDetailList.add(columns[1] + COLUMN_SEPARATOR + columns[2]
              + COLUMN_SEPARATOR + columns[3]);
          continue;
        }
        // (24)SALESデータの取り出し(1keyにつき1行しか現れない
        if (TYPE_SALES.equals(columns[0])) {
          salesData = columns[1] + COLUMN_SEPARATOR + columns[2];
        }
      }
      // (25)SALESデータがReducerに渡ってきているかチェック
      if (salesData.equals("")) {
        return;
      }
      // (26)SALES_DETAILデータがReducerに渡ってきているかチェック
      if (salesDetailList.size() != 0) {
        for (String detailData : salesDetailList) {
          // (27)Reducerのvalueとして、SALESデータとSALES_DETAILデータの連結文字列をセット
          outValue.set(salesData + COLUMN_SEPARATOR + detailData);
          // (28)最終的なkeyとvalueを出力。このセットを1段目のMapReduceジョブの結果とする
          collector.collect(outKey, outValue);
        }
      }
    }
  }
}
