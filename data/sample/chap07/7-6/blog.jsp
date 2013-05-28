<%@ page import="org.apache.hadoop.hbase.*"%>
<%@ page import="org.apache.hadoop.hbase.client.*"%>
<%@ page import="org.apache.hadoop.hbase.util.*"%>
<%@ page import="org.apache.hadoop.hbase.filter.*"%>
<%@ page import="org.apache.hadoop.conf.*"%>
<html>
<head><meta http-equiv="content-type" content="text/html; charset=utf-8"></head>
<title>blog sample</title>ブログサンプル ブログ表示画面<hr>
<%
HTable table = null;
try{
	// サンプルとしてブログIDを固定値とする。
	String blogId = "01";
	Configuration conf = HBaseConfiguration.create();
	table = new HTable(conf, "blog");
	
	// コメント登録。本JSPにPOSTされたときに処理する
	if("comment".equals(request.getParameter("mode"))) {
		// コメントは記事の行キーに"c"とタイムスタンプを付加したものを行キーとする
		long time = System.currentTimeMillis();
		Put put = new Put(Bytes.toBytes(request.getParameter("rowkey") + "c" + time));
		// タイムスタンプを指定したputの利用例
		put.add(Bytes.toBytes("comment"), Bytes.toBytes("name"), time, Bytes.toBytes(request.getParameter("name")));
		put.add(Bytes.toBytes("comment"), Bytes.toBytes("body"), time, Bytes.toBytes(request.getParameter("body")));
		table.put(put);
		%> コメントの追加が完了しました。<hr><%
	}
	// entryテーブルをscanする
	Scan scan = new Scan();
	scan.addFamily(Bytes.toBytes("entry"));
        scan.setFilter(new PrefixFilter(Bytes.toBytes(blogId)));
	ResultScanner scanner = table.getScanner(scan);
	try{
		for (Result result : scanner) {
			// title、bodyを取り出して画面表示する
			String et = Bytes.toString(result.getValue(Bytes.toBytes("entry"), Bytes.toBytes("title")));
			String eb = Bytes.toString(result.getValue(Bytes.toBytes("entry"), Bytes.toBytes("body")));
			%><font color="blue" size="+2"><%= et %></font><br>
			<%= eb %><br><br>
			<dl><dt>コメント</dt><%
			// 記事に紐づいたコメントを表示する
			String srow = Bytes.toString(result.getRow());
			// Scanコンストラクタの例。PrefixFilter(result.getRow())で代用可能。
			Scan cs = new Scan(Bytes.toBytes(srow + "c"), Bytes.toBytes(srow + "c" + Long.MAX_VALUE));
			cs.addFamily(Bytes.toBytes("comment"));
			ResultScanner cscan = table.getScanner(cs);
			try{
				for (Result cr : cscan) {
					// 名前とコメントを取り出して画面表示する
					String cn = Bytes.toString(cr.getValue(Bytes.toBytes("comment"), Bytes.toBytes("name")));
					String cb = Bytes.toString(cr.getValue(Bytes.toBytes("comment"), Bytes.toBytes("body")));
					%><dd><%= cn %>: <%= cb %></dd><%
				}
			} finally {
				cscan.close();
			}
			%><dd>
			<form method="post" action="blog.jsp" name="entry_form">
				<input type="hidden" name="mode" value="comment">
				<input type="hidden" name="rowkey" value="<%= Bytes.toString(result.getRow()) %>">
				名前:<input id="name" type="text" name="name" size="8">
				コメント:<input id="body" type="text" name="body" size="25">
				<input type="submit" value="送信">
			</form></dd></dl><hr><%
		}
	} finally {
		scanner.close();
	}
} finally {
	table.close();
}
%>
</html>

