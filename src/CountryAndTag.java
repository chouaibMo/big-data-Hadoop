
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class CountryAndTag implements WritableComparable<CountryAndTag> {

	private String country ;
	private String tag;	

	
	public CountryAndTag() {
		super();
		tag = "";
		country = "";
	}
	
	public CountryAndTag(String pays, String tag) {
		this.tag = tag;
		this.country = pays;
	}
	
	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}
	

	@Override
	public void readFields(DataInput in) throws IOException {
		Text t = new Text();
		t.readFields(in);
		country = t.toString();
		t.readFields(in);
		tag = t.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text t = new Text(country);
		t.write(out);
		t = new Text(tag);
		t.write(out);
	}

	@Override
	public int compareTo(CountryAndTag o) {
		int r =country.compareTo(o.country) ;
		return r==0?tag.compareTo(o.tag):r;
	}
	
	@Override
	public String toString() {
		return "PaysAndTags ["
				+ "pays=" + country + 
				", tag=" + tag + "]";
	}

}