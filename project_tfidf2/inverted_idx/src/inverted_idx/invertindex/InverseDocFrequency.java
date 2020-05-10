package inverted_idx.invertindex;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.graalvm.compiler.word.Word;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;


public class InverseDocFrequency extends Configured implements Tool {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private static final Pattern word_boundary = Pattern.compile("\\s*\\b\\s*"); // word  boundary start, end, midway
		public void map(LongWritable LineNumber, Text line, Context context) throws IOException, InterruptedException {
			
			String lineString = line.toString();
			
//			FileSplit InputfileSplit = (FileSplit) context.getInputSplit();
//			String fileName = InputfileSplit.getPath().getName();
			
			String[] temp = lineString.split("#####");
			String token = temp[0];
	        String[] NormFrequency = temp[1].split("\t");
	        String hashURL = NormFrequency[0];
	        String freq = NormFrequency[1];
	        
			
	        for (String word  : word_boundary.split(lineString)) {
	        	// for all words in the texts, start making key value pairs
//	            if (word.isEmpty()) {
//	               continue;
//	            }	            
	            context.write(new Text(token), new Text(hashURL+"=="+freq)); 
	         }
		}
	}
	
	  public static class Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
	      @Override 
	      public void reduce( Text token,  Iterable<Text > hashURLFreq,  Context context)
	         throws IOException,  InterruptedException {
  
	         double IDF = 0.0;
	         int DF = 0;
	         int TotalDocs = 0;
	         Text delimitedToken = new Text();

	         Configuration config= context.getConfiguration(); 
	         TotalDocs = Integer.valueOf(config.get("length"));
	         // stores the incoming key and it's value: hashURL==freq 
	         //used to bookkeep the counts of tokens per document. -> hence to calculate DF
	         HashMap<String, String> tokenOccurenceMap = new HashMap<String,String>();

	         for (Text count  : hashURLFreq) {
//	        	 System.out.println(token + " map out " + count);
	        	 
	        	 if(tokenOccurenceMap.containsKey(count.toString()) && (tokenOccurenceMap.containsValue(token.toString()))) {}
	        	 else
	        	 {
	            	 tokenOccurenceMap.put(count.toString(), token.toString());
	            	 DF++;
	            	 }
	            }
	        
//	         System.out.println("end one ----------------- DF " + DF);
	         System.out.println(tokenOccurenceMap);
	         
	         for(String values: tokenOccurenceMap.keySet()){
	        	 String[] parts = values.split("==");
	             IDF = Math.log10(1 + (TotalDocs/DF));	             
	         }
	         
	         String temp1 =  token.toString() + "#####" ;
	         delimitedToken = new Text(temp1);		 
	         context.write(delimitedToken, new DoubleWritable(IDF));
	      
	      }
	   }
	  
//		public int run(String[] args) throws Exception {
////			if (args.length != 2) {
////				System.err.println("Usage: TermFrequency, Please pass correct input args lalalallal");
////				System.exit(2);
////			}
//			
//			Job job = Job.getInstance(getConf(), " TermFrequency ");
//			
//			job.setJarByClass(this.getClass());
//			job.setMapperClass(Map.class);
//			job.setReducerClass(Reduce.class);
//			job.setOutputKeyClass(Text.class);
//			job.setMapOutputValueClass(DoubleWritable.class);
//			job.setOutputValueClass(DoubleWritable.class);
//			
//			FileInputFormat.addInputPath(job, new Path(args[0]));
//			FileOutputFormat.setOutputPath(job, new Path(args[1]));
//			return (job.waitForCompletion(true) ? 0 : 1);
//		}
		
	  
	   public int run( String[] args) throws  Exception {
//		   FileSystem filesystem = FileSystem.get(getConf());
//		   Path path = new Path(args[0]);
//		   ContentSummary contentsummary = filesystem.getContentSummary(path);
//		   long fileCount = contentsummary.getFileCount();
		  long fileCount = 8306;
		  Configuration config = new Configuration();
//		  config.set("length", String.valueOf(fileCount));
		  config.set("length", String.valueOf(fileCount));
	      Job job  = Job.getInstance(config, " InverseDocFrequency ");
	      job.setJarByClass(this.getClass());

//	      FileInputFormat.addInputPath(job,  new Path(args[1]));
//	      FileOutputFormat.setOutputPath(job,  new Path(args[ 2]));
	      
	      FileInputFormat.addInputPath(job,  new Path(args[0]));
	      FileOutputFormat.setOutputPath(job,  new Path(args[1]));
	      
	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);
	      job.setOutputKeyClass(Text.class);
	      job.setMapOutputValueClass(Text.class);
	      job.setOutputValueClass(DoubleWritable.class);

	      return job.waitForCompletion(true) ? 0 : 1;
	   }
	  
	
	   
	   public static void main( String[] args) throws  Exception {
		   int res  = ToolRunner.run( new InverseDocFrequency(), args);
	      System.exit(res);
	   }
	
}
