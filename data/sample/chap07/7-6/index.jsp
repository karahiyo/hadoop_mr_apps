<%@ page import="org.apache.hadoop.hbase.*"%>
<%@ page import="org.apache.hadoop.hbase.client.*"%>
<%@ page import="org.apache.hadoop.hbase.util.*"%>
<%@ page import="org.apache.hadoop.conf.*"%>

<html>
<head><meta http-equiv="content-type" content="text/html; charset=utf-8"></head>
<title>blog sample</title>ブログサンプル ブログ記事登録画面<hr>
<%
	// サンプルとしてブログIDを固定値とする。
	String blogId = "01";
	Configuration conf = HBaseConfiguration.create();
	HTable table = new HTable(conf, "blog");
	
	if("add".equals(request.getParameter("mode"))) {
		Put put = new Put(Bytes.toBytes(blogId + (Long.MAX_VALUE - System.currentTimeMillis())));
		put.add(Bytes.toBytes("entry"), Bytes.toBytes("title"), Bytes.toBytes(request.getParameter("title")));
		put.add(Bytes.toBytes("entry"), Bytes.toBytes("body"), Bytes.toBytes(request.getParameter("body")));
		table.put(put);
		%>  記事の追加が完了しました。<br><%
	} else {
%>
	<form method="post" action="index.jsp" name="entry_form">
		<input type="hidden" name="mode" value="add">
		<dl><dt><label for="title">タイトル:</label></dt><dd>
			<input id="title" type="text" name="title" size="30"></dd>
			<dt><label for="body">本文:</label></dt><dd>
			<textarea id="body" cols="50" rows="8" name="body"></textarea></dd>
		</dl>
		<input type="submit" value="送信">
	</form>
<%	}%>
<a href="blog.jsp">ブログ表示画面へ。</a>
</html>

