package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class TFidfRecordReader extends RecordReader<Text, Text> {

	private static LineRecordReader linerecReader;
	private Text keyInputTfidf;
	private Text valueinputTfidf;
	
	public TFidfRecordReader(InputSplit inputSplit, TaskAttemptContext contxt) throws IOException, InterruptedException {
		// TODO Auto-generated constructor stub
		linerecReader = new LineRecordReader();
		this.initialize(inputSplit, contxt);
	}
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext contxt) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		linerecReader.initialize(inputSplit, contxt);
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(linerecReader.nextKeyValue() == false)
			return false;
		String[] lineTerms = linerecReader.getCurrentValue().toString().split("\t");// tab separated keyvalues
	
	
	if (lineTerms.length != 2)
		throw new IOException("Wrong Input idf");
			keyInputTfidf = new Text(lineTerms[0]);
			valueinputTfidf = new Text(lineTerms[1]);
		return true;	
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		linerecReader.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return keyInputTfidf;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return valueinputTfidf;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		
		return linerecReader.getProgress();
	}

	}
