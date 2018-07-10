package tfidf;
import java.io.IOException;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;



public class IDFInputformat extends FileInputFormat<Text, IntWritable> {

	

	@Override
	public RecordReader<Text, IntWritable> createRecordReader(InputSplit input, TaskAttemptContext contxt)
			throws IOException, InterruptedException {
		// return type is recordReader<text,intWritable>
		contxt.setStatus(input.toString());
		//calls another class
		return new IDFRecordReader((FileSplit) input, contxt);
	}

}
