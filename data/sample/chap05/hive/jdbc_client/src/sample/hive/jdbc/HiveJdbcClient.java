package sample.hive.jdbc;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {

  /* Hive-JDBCドライバ */
  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

  public static void main(String[] args) {

    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    Connection con = null;
    try {
      String connectionURL = "jdbc:hive://localhost:10000/default";
      // Connectionの取得
      con = DriverManager.getConnection(connectionURL, "", "");
      Statement stmt = con.createStatement();
      // スキーマの切り替え
      stmt.executeQuery("use SALES_SAMPLE");
      // 実行するSQL
      String sql = "SELECT SHOP_CODE, COUNT(*) FROM SALES "
          + "WHERE SALES_DATE LIKE '2012-02%' "
          + "GROUP BY SHOP_CODE ORDER BY SHOP_CODE";

      ResultSet res = stmt.executeQuery(sql);
      // 結果セットの表示
      while (res.next()) {
        String shopCode = res.getString(1);
        String count = res.getString(2);
        System.out.println("RESULT : " + shopCode + "\t" + count);
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (con != null) {
        try {
          con.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
