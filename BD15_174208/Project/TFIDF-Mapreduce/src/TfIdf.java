package tfidf;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TfIdf {

	public  static class TfidfMapper extends Mapper<Text, Text, Text, DoubleWritable>
	{
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException
		{
		String[] noDocterm_docCode_termsInDoc = value.toString().split("_");
		// 
		 long allTerms = context.getConfiguration().getLong("allTerms", 0);
		long allDocuments = context.getConfiguration().getLong("allDocs", 0);
		
				double tf = ((double)Integer.parseInt(noDocterm_docCode_termsInDoc[2])/allTerms);
				System.out.println(noDocterm_docCode_termsInDoc);
				System.out.println(allTerms);
				System.out.println(allDocuments);
		double idf = Math.log((double)allDocuments/(double)Integer.parseInt(noDocterm_docCode_termsInDoc[0]));
		double tfidf =tf * idf;
		
		context.write(new Text(noDocterm_docCode_termsInDoc[1]+"_"+ key), new DoubleWritable(tfidf));
		}
		}
	
public static class TfidfReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException	
{
	for (DoubleWritable d: values)
		context.write(key, d);
		
}
}
}