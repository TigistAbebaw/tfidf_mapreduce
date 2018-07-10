package tfidf;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class InverseDocFrequecy {
 public static class IdfMapper extends Mapper<Text, IntWritable, Text, Text> 
 
 {
	 //number of documents that contain the word
	 private Text term = new Text();
	 private Text output = new Text();
	 
	 public void map(Text key, IntWritable value, Context contex) throws IOException, InterruptedException
	 {
		 String[] docName_term = key.toString().split("_");//docCode_term
		 term.set(docName_term[1]);
		 output.set(docName_term[0]+"_"+value);//docCode_termFreq
		 contex.write(term, output);
		 
	 }
 }	 
	 public static class IdfReducer extends Reducer<Text, Text, Text, Text>
	 
	 {
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		 {
			 ArrayList<Text> valueArray = new ArrayList<Text>();
			 for(Text t: values)
			 {  //size of iterable
				 valueArray.add(new Text(t));
			 }
			 //where does it collect
			 for(Text t : valueArray)
				 context.write(key, new Text(valueArray.size()+"_"+ t.toString()));
			 //docCount_docId_countInDoc
		 }
		 
	 }
	 
 }

