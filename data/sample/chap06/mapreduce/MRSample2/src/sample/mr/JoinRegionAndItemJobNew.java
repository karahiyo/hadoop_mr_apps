package sample.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 2段目のMapReduceジョブ
 * 1段目のMapReduceジョブの結果に対して、店舗マスタの地域を結合する
 */
public class JoinRegionAndItemJobNew {

  /**
   * カラムセパレータ文字列
   */
  private static final String COLUMN_SEPARATOR = "\t";

  /**
   * 連結した要素のセパレータ文字列
   */
  private static final String ELEMENT_SEPARATOR = ":";

  /**
   * SHOPLISTシンボリックリンク名
   */
  private static final String DIST_SHOP_CACHE_LINK = "shoplist.tsv";

  /**
   * ITEMLISTシンボリックリンク名
   */
  private static final String DIST_ITEM_CACHE_LINK = "itemlist.tsv";

  public static void main(String[] args) {

    try {
      // (1)Configurationオブジェクトの生成
      Configuration conf = new Configuration();
      // (2)汎用オプションの解析
      String[] otherArgs = new GenericOptionsParser(conf, args)
          .getRemainingArgs();
      if (otherArgs.length != 4) {
        System.err
            .println("Usage: sample.mr.JoinRegionAndItemJobNew <job1_output> <out> <shoplist> <itemlist>");
        System.exit(2);
      }

      // (3)ジョブ起動用オブジェクトの生成
      Job job = new Job(conf, "JoinRegionJobNew");
      // (4)本クラスを含むjarファイルをジョブに登録(これによりスレーブノードのクラスパスにjarが組み込まれる)
      job.setJarByClass(JoinRegionAndItemJobNew.class);
      // (5)最終結果のKeyクラスを指定
      job.setOutputKeyClass(Text.class);
      // (6)最終結果のValueクラスを指定
      job.setOutputValueClass(Text.class);
      // (7)Mapperクラスを指定
      job.setMapperClass(JoinRegionAndItemJobMapper.class);
      // (8)Reducerクラスを指定
      job.setReducerClass(JoinRegionAndItemJobReducer.class);
      // (9)DistributedCache用のファイルを指定
      // #以降のDIST_CACHE_LINKの名前でMapタスク実行マシンのローカルファイルシステムに
      // 分散キャッシュファイルのシンボリックリンクが作成される
      // シンボリックリンクを作成することで、Mapタスクからシンボリックリンク名でアクセスできる
      String shopCacheFilePath = new Path(args[2]).toString() + "#"
          + DIST_SHOP_CACHE_LINK;
      DistributedCache.addCacheFile(new URI(shopCacheFilePath),
          job.getConfiguration());
      String itemCacheFilePath = new Path(args[3]).toString() + "#"
          + DIST_ITEM_CACHE_LINK;
      DistributedCache.addCacheFile(new URI(itemCacheFilePath),
          job.getConfiguration());

      // (10)シンボリックリンクの作成
      DistributedCache.createSymlink(job.getConfiguration());
      // (11)入力データ格納ディレクトリを指定
      FileInputFormat.addInputPath(job, new Path(args[0]));
      // (12)出力データ格納ディレクトリを指定
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      // (13)ジョブの実行を開始
      boolean result = job.waitForCompletion(true);
      System.exit(result ? 0 : 2);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * mapメソッドを実装したMapperクラス 
   * ドライバクラス内に静的ネストクラスとして定義 
   * 新APIではMapperクラスを拡張するのみ
   */
  public static class JoinRegionAndItemJobMapper extends
      Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * 店舗情報格納用HashMap
     */
    private Map<String, String> regionMap = new HashMap<String, String>();

    /**
     * 商品情報格納用HashMap
     */
    private Map<String, String> itemMap = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {

      // (14)DistributedCacheを読み込み、メモリ(HashMap)に格納する
      // シンボリックリンクが作成されているため、ソースの冒頭で指定したリンク名で直接アクセス可能
      File shopCacheFile = new File(DIST_SHOP_CACHE_LINK);
      BufferedReader reader = null;

      try {
        reader = new BufferedReader(new FileReader(shopCacheFile));
        String line = null;
        while ((line = reader.readLine()) != null) {
          if (line.equals("")) {
            continue;
          }
          String[] columns = line.split(COLUMN_SEPARATOR);
          String shopCode = columns[0];
          String region = columns[1];
          regionMap.put(shopCode, region);
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }

      File itemCacheFile = new File(DIST_ITEM_CACHE_LINK);
      try {
        reader = new BufferedReader(new FileReader(itemCacheFile));
        String line = null;
        while ((line = reader.readLine()) != null) {
          if (line.equals("")) {
            continue;
          }
          String[] columns = line.split(COLUMN_SEPARATOR);
          String itemCode = columns[0];
          String itemName = columns[1];
          itemMap.put(itemCode, itemName);
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      // (15)valueの取得。
      // 　　valueにテキストファイル1行分のデータが格納されている
      String line = value.toString();
      if (line == null || line.equals("")) {
        return;
      }

      line = line.trim();
      // (16)行テキストデータをスペース、タブ文字で分割
      String[] columns = line.split(COLUMN_SEPARATOR);
      String shopCode = columns[1];
      String salesDate = columns[2];
      String itemCode = columns[3];
      int unitPrice = Integer.parseInt(columns[4]);
      int quantity = Integer.parseInt(columns[5]);

      // (17)shopCodeからregionを取得する
      String region = regionMap.get(shopCode);
      if (region == null) {
        System.out.println(shopCode + " missing");
      }
      // (18)itemCodeからitemNameを取得する
      String itemName = itemMap.get(itemCode);
      // (19)Mapperから出力するkeyを設定
      outKey.set(itemCode + ELEMENT_SEPARATOR + itemName + COLUMN_SEPARATOR
          + region + COLUMN_SEPARATOR + salesDate);
      // (20)Mapperから出力するvalueを設定
      outValue.set(quantity + ELEMENT_SEPARATOR + (unitPrice * quantity));
      // (21)key/valueを出力
      context.write(outKey, outValue);
    }
  }

  /**
   * reduceメソッドを実装したReducerクラス 
   * ドライバクラス内に静的ネストクラスとして定義 
   * 新APIではReducerクラスを拡張するのみ
   */
  public static class JoinRegionAndItemJobReducer extends
      Reducer<Text, Text, Text, Text> {

    Text outKey = new Text();
    Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // (22)Reducerに集約されたkeyを取得
      String keyStr = key.toString();
      int totalQuantity = 0;
      int totalPrice = 0;

      // (23)集約されたvalueのリストでループし、valueとして渡されたカウントを足し合わせる
      Iterator<Text> ite = values.iterator();
      while (ite.hasNext()) {
        String[] valueColumns = ite.next().toString().split(ELEMENT_SEPARATOR);
        int quantity = Integer.parseInt(valueColumns[0]);
        int price = Integer.parseInt(valueColumns[1]);
        totalQuantity += quantity;
        totalPrice += price;
      }
      // (24)Reducerのkeyをセット
      outKey.set(keyStr);
      // (25)Reducerのvalueとして、合計した個数と合計した金額をセット
      outValue.set(totalQuantity + COLUMN_SEPARATOR + totalPrice);
      // (26)最終的なkeyとvalueを出力。このセットを2段目のMapReduceジョブの結果とする
      context.write(outKey, outValue);
    }
  }
}
