package HelloWorld;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class XMLInputFormat extends TextInputFormat{

	 public RecordReader<LongWritable, Text> createRecordReader(
	            InputSplit split, TaskAttemptContext context) {
	        return new XMLRecordReader();
	
	 }
	
}
