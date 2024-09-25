package eu.bosteels.duckdb.attach;

public class RandomString {

  public static String generate(int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append((char) ((int) (Math.random() * 26) + 'A'));
    }
    return sb.toString();
  }

  public static void main(String[] args) {
    String s = RandomString.generate(4000);
    System.out.println("s = " + s);
  }

}
