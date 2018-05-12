package MapReduce3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class BigramProductWritable  implements WritableComparable<BigramProductWritable> {
	private Text leftBigram;
	private Text rightBigram;
	public BigramProductWritable() {
	}
	public BigramProductWritable(Text left, Text right) {
		this.leftBigram = left;
		this.rightBigram = right;
	}
	public void readFields(DataInput in) throws IOException {
		leftBigram = new Text(in.readUTF());
		rightBigram = new Text(in.readUTF());
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(leftBigram.toString());
		out.writeUTF(rightBigram.toString());
	}
	public void set(Text prev, Text cur) {
		leftBigram = prev;
		rightBigram = cur;
	}
	@Override
	public String toString() {
		return leftBigram.toString() + " " + rightBigram.toString();
	}
	@Override
	public int hashCode() {
		return leftBigram.hashCode() + rightBigram.hashCode();
	}
	@Override
	public boolean equals(Object o) {
		if (o instanceof BigramProductWritable) {
			BigramProductWritable bigram = (BigramProductWritable) o;
			return leftBigram.equals(bigram.leftBigram)
					&& rightBigram.equals(bigram.rightBigram);
		}
		return false;
	}
	public int compareTo(BigramProductWritable tp) {
		int cmp = leftBigram.compareTo(tp.leftBigram);
		if (cmp != 0) {
			return cmp;
		}
		return rightBigram.compareTo(tp.rightBigram);
	}

}
