package tfidf;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class TermFreq {

	//to be visible only in this class and subclasses
	protected static boolean caseSensitive = false;
	
	
	// If a class is useful to only one other class, then it is logical to embed 
	//it in that class and keep the two together.
	public static class TermFrequencyMapper extends Mapper<Object, Text, Text, IntWritable>{
	//Set<> is a collection with no dublicate elements
	//Here it is used to store unique terms collection
		private Set<String> docCodesCollection = new HashSet<String>();
		private final static IntWritable one = new IntWritable(1);
		private Text term = new Text();
		
		public void map(Object key,Text value, 	Context context) throws IOException, InterruptedException
		{
			String lineRead = value.toString().toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(lineRead, ", .;:'\"&!?-_\n\t12345678910[]{}<>\\`~|=^()@#$%^*/+-");
			
			while(tokenizer.hasMoreTokens())
			{  // instead of putting in array list which will double the computation
				//we concatenate the document id with the text
				//so same terms in one document will be identified
				term.set(getDocumentHashcode(context)+"_"+ tokenizer.nextToken());
				context.write(term, one);
				// linereader just reads through every doc without stop
				//for tokens instead of initializing an array
				context.getCounter(TfidfAggregator.mycounters.TERMS).increment(1);
				
			}
			
		}

		private String getDocumentHashcode(Context context) {
			//multiple input files stored in the input directory, each mapper may read a
			//different file, and I need to know which file the mapper has read.
			
			//parse to fileSplit class, split the whole doc in to files
			//InputSplit represents the data to be processed by an individual Mapper(upto the record reader to figure this out)
			//But in order to get the file path and the file name you will need to first typecast the result into FileSplit.

//
			//So, in order to get the input file path you may do the following 
			String dochashCode = ((FileSplit) context.getInputSplit()).getPath().getName();
			if(!(docCodesCollection.contains(dochashCode)))
					{
				docCodesCollection.add(dochashCode);
				context.getCounter(TfidfAggregator.mycounters.DOCUMENTS).increment(1);
				
					}
			return dochashCode;
		}
	}
		public static class TermFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable>
		{
			public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
				int allTermsDoc = 0;
				for(IntWritable val: values)
					allTermsDoc += val.get();
				context.write(key, new IntWritable(allTermsDoc));
			}
		}
		
	}
	
	

