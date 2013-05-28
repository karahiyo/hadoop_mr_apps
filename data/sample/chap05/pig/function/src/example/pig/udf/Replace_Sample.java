package example.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

//関数名をクラス名にします
public class Replace_Sample extends EvalFunc<String> {

  @Override
  public String exec(Tuple input) throws IOException {
    // nullまたは空文字の場合はnullを返す
    if (input == null || input.size() == 0) {
      return null;
    }
    try {
      // 引数inputには、Replace_Sample関数への引数が順に格納されている
      // 1番目の要素(置換処理対象文字列)を取得
      String fieldValue = (String) input.get(0);
      // 2番目の要素(置換元文字列)を取得
      String symbolFrom = (String) input.get(1);
      // 3番目の要素(置換先文字列)を取得
      String symbolTo = (String) input.get(2);
      // fieldがnullもしくは空文字であればnullを返す
      if (fieldValue == null || fieldValue.equals("")) {
        return null;
      }
      // 置換元文字がnullもしくは空文字であればfieldをそのまま返す
      if (symbolFrom == null || symbolFrom.equals("")) {
        return fieldValue;
      }
      // 置換先文字がnullもしくは空文字であればfieldをそのまま返す
      if (symbolTo == null || symbolTo.equals("")) {
        return fieldValue;
      }
      // 文字列の置換
      String replacedStr = fieldValue.replaceAll("\\Q" + symbolFrom + "\\E",
          symbolTo);
      return replacedStr;

    } catch (ExecException e) {
      throw new IOException(e);
    }
  }
}
