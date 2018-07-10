package tfidf;


import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class IDFRecordReader extends RecordReader<Text, IntWritable> {

	private static org.apache.hadoop.mapreduce.lib.input.LineRecordReader linerecReaderIdf;

	private Text keyinputIdf;
	private IntWritable valueinputIdf;
		
	
	//InputSplit the split that defines the range of Records to read
	public IDFRecordReader(InputSplit inputsplit, TaskAttemptContext contxt) throws IOException,InterruptedException {
		linerecReaderIdf = new org.apache.hadoop.mapreduce.lib.input.LineRecordReader();
		this.initialize(inputsplit, contxt);
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext contxt) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		linerecReaderIdf.initialize(inputSplit,contxt);
	}
	public void close() throws IOException {
		linerecReaderIdf.close();
	}

	
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return keyinputIdf;
	}

	@Override
	public IntWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return valueinputIdf;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return linerecReaderIdf.getProgress();
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if(linerecReaderIdf.nextKeyValue() == false)
				return false;
		//store values of text in array key,value
		String[] lineTerms = linerecReaderIdf.getCurrentValue().toString().split("\t");
		
	
		
		if (lineTerms.length != 2)
			throw new IOException("wrong input idf");
		keyinputIdf = new Text(lineTerms[0]);
		valueinputIdf = new IntWritable(Integer.parseInt(lineTerms[1]));
		
		return true;
	
	
	}

	
	
	

}
