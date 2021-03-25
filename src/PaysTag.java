import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PaysTag extends WritableComparator implements WritableComparable<PaysTag> {

	private Text pays;
	private Text tag;

	public PaysTag() {
		super(StringAndInt.class, true);
		this.pays = new Text();
		this.tag = new Text();
	}

	public PaysTag(String pays, String tag) {
		this.pays = new Text(pays);
		this.tag = new Text(tag);

	}

	public Text getPays() {
		return pays;
	}

	public void setPays(Text pays) {
		this.pays = pays;
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = tag;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pays.readFields(in);
		tag.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		pays.write(out);
		tag.write(out);

	}

	@Override
	public int compareTo(PaysTag o) {
		if (pays.toString().equals(o.pays.toString())) {
			return tag.toString().compareTo(o.tag.toString());
		}
		return pays.toString().compareTo(o.pays.toString());
	}

	@Override
	public String toString() {
		return pays.toString() + " " + tag.toString();
	}
}