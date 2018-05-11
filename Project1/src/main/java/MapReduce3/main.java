package MapReduce3;
import org.apache.hadoop.io.Text;

public class main {
	
	public static void main(String[] args) {
		Text t = new Text("0006641040-2003");
		String [] s = t.toString().split("-");
		System.out.println(s[1]);
		
		
	}

}
