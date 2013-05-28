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
 * sales_detail内での合計金額の計算、およびsalesデータとの結合
 */
public class JoinSalesDataJob {

    /** SALESデータ識別用文字列 */
    private static final String TYPE_SALES = "sales";

    /** SALES_DETAILデータ識別用文字列 */
    private static final String TYPE_SALES_DETAIL = "detail";

    /** カラムセパレータ文字列 */
    private static final String COLUMN_SEPARATOR = "\t";

    public static void main(String[] args) {

        try {
            // Configurationオブジェクトの生成
            Configuration conf = new Configuration();
            // 汎用オプションの解析
            String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
            if (otherArgs.length != 3) {
                System.err.println("Usage: sample.mr.JoinSalesData <sales> <sales_detail> <out>");
                System.exit(2);
            }

            // ジョブ起動用オプションの生成
            Job job = new Job(conf, "JoinSalesDataJob");
            // 本クラスを含むjarファイルをジョブに登録
            // (これにより、スレーブノードのクラスパスにjarが組み込まれる)
            job.setJarByClass(JoinSalesDataJob.class);
            // 最終結果のKeyクラスを指定
            job.setOutputKeyClass(Text.class);
            // 最終結果のValueクラスを指定
            job.setOutputValueClass(Text.class);
            // MultipleInputsによるsalesデータの入力指定
            MultipleInputs.addInputPath(job, new Path(args[0]),
                    TextInputFormat.class, ForSalesDataMapper.class);
            // MultipleInputsによるsales_detailデータの入力指定
            MultipleInputs.addInputPath(job, new Path(args[1]),
                    TextInputFormat.class, ForSalesDetailDataMapper.class);

            // Reducerクラスを指定
            job.setReducerClass(JoinSalesDataJobNewReducer.class);
            // 出力データ格納用ディレクトリを指定
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            // ジョブの実行を開始
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * mapメソッドを実装した売上データ用Mapperクラス。
     * ドライバクラス内に静的ネストクラスとして定義
     * 新APIでは、Mapperクラスを拡張するのみ
     */
    public static class ForSalesDataMapper extends
        Mapper<LongWritable, Text, Text, Text> {

            private Text outKey = new Text();
            private Text outValue = new Text();

            @Override
            protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

                // valueの取得
                String line = value.toString();
                if (line == null || line.equals("")) {
                    return;
                }

                line = line.trim();
                // 行テキストデータをスペース、タブで分割
                String[] columns = line.split(COLUMN_SEPARATOR);
                String salesId = columns[0];
                String shopCode = columns[1];
                String salesData = columns[2];
                // Mapperから出力するkeyを設定
                outKey.set(salesId);
                // Mapperから出力するValueを設定
                outValue.set(TYPE_SALES + COLUMN_SEPARATOR + shopCode +
                        COLUMN_SEPARATOR + salesData);
                // Key/Valueを出力
                context.write(outKey, outValue);
            }
        }

    /**
     *  mapメソッドを実装した売上明細用Mapperクラス
     *  ドライバクラス内に静的ネストクラスとして定義
     *  新APIではMapperクラスを拡張するのみ
     */
    public static class ForSalesDetailDataMapper extends
        Mapper<LongWritable, Text, Text, Text> {

            private Text outKey = new Text();
            private Text outValue = new Text();

            @Override
            protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

                // valueの取得
                String line = value.toString();
                if (line == null || line.equals("")) {
                    return;
                }

                line = line.trim();
                // 行テキストデータをスペース、タブで分割
                String[] columns = line.split(COLUMN_SEPARATOR);
                String salesId = columns[0];
                String itemCode = columns[2];
                int unitPrice = Integer.parseInt(columns[3]);
                int quantity = Integer.parseInt(columns[4]);

                // Mapperから出力するKey/Valueを設定
                outKey.set(salesId);
                outValue.set(TYPE_SALES_DETAIL + COLUMN_SEPARATOR + itemCode +
                        COLUMN_SEPARATOR + unitPrice + COLUMN_SEPARATOR + quantity);
                //   Key/Valueを出力
                context.write(outKey, outValue);
            }
        }


    /**
     * reduceメソッドを実装したReducerクラス
     * ドライバクラス内に静的ネストクラスとして定義
     */
    public static class JoinSalesDataJobNewReducer extends
        Reducer<Text, Text, Text, Text> {

            Text outKey = new Text();
            Text outValue = new Text();

            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
                // Reducerに集約されたkeyを取得
                String salesId = key.toString();
                outKey.set(salesId);
                List<String> salesDetailList = new ArrayList<String>();
                String salesData = "";

                // 集約されたvalueのリストでループし、salesもしくはsales_detailに
                // 対する処理を行う
                Iterator<Text> ite = values.iterator();
                while(ite.hasNext()) {
                    String value = ite.next().toString();
                    String[] columns = value.split(COLUMN_SEPARATOR);

                    // SALES_DETAILデータの取り出し
                    if (TYPE_SALES_DETAIL.equals(columns[0])) {
                        salesDetailList.add(columns[1] + COLUMN_SEPARATOR
                            + columns[2] + COLUMN_SEPARATOR + columns[3]);
                        continue;
                    }
                    // SALESデータの取り出し
                    if (TYPE_SALES.equals(columns[0])) {
                        salesData = columns[1] + COLUMN_SEPARATOR + columns[2];
                    }
                }

                // SALESデータがReducerに渡ってきているかチェック
                if (salesData.equals("")) {
                    return;
                }

                // SALES_DETAILデータがReducerに渡ってきているかチェック
                if (salesDetailList.size() != 0) {
                    for (String detailData : salesDetailList) {
                        // ReducerのValueとして、SALESデータと
                        // SALES_DETAILデータの連結文字列をセット
                        outValue.set(salesData + COLUMN_SEPARATOR + detailData);
                        // 最終的なkeyとvalueを出力。
                        // このセットを一段目のMapReduceジョブの結果とする
                        context.write(outKey, outValue);
                    }
                }
            }
        }
}

