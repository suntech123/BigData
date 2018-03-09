package sunspark_hbase.HBaseSampleApp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RetriveData {

	public static void main(String[] args) throws IOException {
		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();

		// Instantiating HTable class
		HTable table = new HTable(config, "emp");

		// Instantiating Get class
		Get g = new Get(Bytes.toBytes("row1"));

		// Reading the data
		Result result = table.get(g);

		// Reading values from Result class object
		byte[] value = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"));
		byte[] value1 = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"));
		byte[] value2 = result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("designation"));
		byte[] value3 = result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("salary"));

		// Printing the values
		String name = Bytes.toString(value);
		String city = Bytes.toString(value1);
		String designation = Bytes.toString(value2);
		String salary = Bytes.toString(value3);

		System.out.println("name: " + name + " city: " + city);
		System.out.println("designation: " + designation + " salary: " + salary);

	}

}
