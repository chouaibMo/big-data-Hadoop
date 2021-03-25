import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StringAndInt implements WritableComparable<StringAndInt> {
	public Text text;
	public int occurence;
	
	
	public int getOccurrence() {
		return occurence;
	}

	public void setOccurrence(int occurences) {
		this.occurence = occurences;
	}

	public Text getText() {
		return text;
	}

	public void setText(Text tag) {
		this.text = tag;
	}

	

	public StringAndInt(String tag, int occurences) {
		super();
		this.text = new Text(tag);
		this.occurence = occurences;
	}

	public StringAndInt() {
		this.text = new Text();
		this.occurence = 0;
	}
	
	@Override
	public int compareTo(StringAndInt arg0) {
		if (occurence > arg0.occurence)
			return -1;
		else if (occurence < arg0.occurence)
			return 1;
		else
			return 0;
	}
	
	public String toString() {
		return this.text.toString() + " " +this.occurence +" ";
		
	}	
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.text.set(arg0.readUTF());
		this.occurence = arg0.readInt();       
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(text.toString());
		arg0.writeInt(occurence);	
	}

}