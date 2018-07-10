package tfidf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import tfidf.TermFreq.TermFrequencyMapper;

public class TfidfAggregator {
	static enum mycounters{
		DOCUMENTS,TERMS
	}
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration();
		String[] fileArgs = null;
		try{
		fileArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	//to take command line arguments
		}
		catch(NullPointerException e)
		{
			System.err.println("Please insert Inputfilename and outputfilename");
		}
		
		String inputfile = fileArgs[0];
		String outputTF = fileArgs[1]+"TF";
		String outputIDF = fileArgs[1]+"IDF";
		String outputTFIDF = fileArgs[1]+"TFIDF";
		
		Job termFrequencyJob = new Job(conf, "termfrequency");
		termFrequencyJob.setJarByClass(TermFreq.class);
		termFrequencyJob.setMapperClass(TermFrequencyMapper.class);
		termFrequencyJob.setReducerClass(TermFreq.TermFrequencyReducer.class);
		termFrequencyJob.setOutputFormatClass(TextOutputFormat.class);
		termFrequencyJob.setOutputKeyClass(Text.class);
		termFrequencyJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(termFrequencyJob, new Path(inputfile));
		FileOutputFormat.setOutputPath(termFrequencyJob, new Path(outputTF));		
		
		Job InverseDocFrequecyJob = new Job(conf, "InverseDocFrequecy");
		InverseDocFrequecyJob.setJarByClass(InverseDocFrequecy.class);
		InverseDocFrequecyJob.setMapperClass(InverseDocFrequecy.IdfMapper.class);
		InverseDocFrequecyJob.setReducerClass(InverseDocFrequecy.IdfReducer.class);
		InverseDocFrequecyJob.setInputFormatClass(IDFInputformat.class);//plain text by line number
		InverseDocFrequecyJob.setOutputKeyClass(Text.class);
		InverseDocFrequecyJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(InverseDocFrequecyJob, new Path(outputTF));
		FileOutputFormat.setOutputPath(InverseDocFrequecyJob,new Path(outputIDF));
		
		
		Job TFIDFJob = new Job(conf, "TFIDF");
		TFIDFJob.setJarByClass(TfIdf.class);
		TFIDFJob.setMapperClass(TfIdf.TfidfMapper.class);
		TFIDFJob.setCombinerClass(TfIdf.TfidfReducer.class);
		TFIDFJob.setReducerClass(TfIdf.TfidfReducer.class);//why reducer
		TFIDFJob.setInputFormatClass(TfidfInputFormat.class);
		TFIDFJob.setOutputKeyClass(Text.class);
		TFIDFJob.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(TFIDFJob, new Path(outputIDF));
		FileOutputFormat.setOutputPath(TFIDFJob, new Path(outputTFIDF));

		
		if(termFrequencyJob.waitForCompletion(true))
		{
			long allTerms = termFrequencyJob.getCounters().findCounter(mycounters.TERMS).getValue();
			long allDocs = termFrequencyJob.getCounters().findCounter(mycounters.DOCUMENTS).getValue();
			
			if(InverseDocFrequecyJob.waitForCompletion(true))
			{
				TFIDFJob.getConfiguration().setLong("allTerms", allTerms);
				TFIDFJob.getConfiguration().setLong("allDocs", allDocs);
				System.exit(TFIDFJob.waitForCompletion(true)? 0: 1);
			}
		}
		
		System.exit(1);
	
	}
	
	
}
