import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {

	public SortComparator() {
		super(StringAndInt.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		StringAndInt s1 = (StringAndInt) a;
		StringAndInt s2 = (StringAndInt) b;
		
		
		if (s1.getText().compareTo(s2.getText()) == 0) {
			return s2.getOccurrence() - s1.getOccurrence();
		}
		return s1.getText().compareTo(s2.getText());
	}

}