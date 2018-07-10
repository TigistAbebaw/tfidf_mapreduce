package tfidf;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TfidfInputFormat extends FileInputFormat<Text, Text> {

	

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit input, TaskAttemptContext contxt)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		contxt.setStatus(input.toString());
		return new TFidfRecordReader((FileSplit) input, contxt);
	}
}


