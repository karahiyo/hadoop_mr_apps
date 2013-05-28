●はじめに
~/mapreduce/MRSample2-oldディレクトリが存在し、
~/mapreduce/MRSample2-old/src/sample/mr/ディレクトリに
旧APIの以下のソースコードが格納されていることを前提としています。
・JoinRegionAndItemJobOld.java
・JoinSalesDataJobOld.java

●コンパイル-実行手順
mruserにて以下のコマンドを実行していってください。

(1)コンパイル
cd ~/mapreduce/MRSample2-old
mkdir classes
★CDH3の場合
　CLASSPATH=/usr/lib/hadoop/hadoop-core.jar:/usr/lib/hadoop/lib/commons-cli-1.2.jar
★CDH4の場合
　CLASSPATH=/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-core.jar:/usr/lib/hadoop/hadoop-common.jar:/usr/lib/hadoop/lib/commons-cli-1.2.jar

javac -classpath $CLASSPATH -d classes ./src/sample/mr/Join*Old.java

(2)Jarファイルの作成
cd ~/mapreduce/MRSample2-old/classes
jar cvf ../CalcRegionAggregationOld.jar sample

(3)一つ目のジョブを実行
cd ~/mapreduce/MRSample2-old
hadoop jar CalcRegionAggregationOld.jar sample.mr.JoinSalesDataJobOld input_sales input_sales_detail output_old_job1

(4)二つ目のジョブを実行
hadoop jar CalcRegionAggregationOld.jar sample.mr.JoinRegionAndItemJobOld output_old_job1 output_old_job2 input_shoplist/shoplist.tsv input_itemlist/itemlist.tsv

(5)結果の確認
hadoop fs -cat /user/mruser/output_old_job2/part-00000 | head
