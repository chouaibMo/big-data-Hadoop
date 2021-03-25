import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CountryComparator extends WritableComparator {

	public CountryComparator() {
		super(StringAndInt.class, true);
	}

	/*
	 * Cette methode permet de comparer 2 pays.
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		StringAndInt s1 = (StringAndInt) a;
		StringAndInt s2 = (StringAndInt) b;
		return s1.getText().compareTo(s2.getText());
	}

}