package inverted_idx.invertindex;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
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
import java.text.Normalizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;


//class FormattedDateMatcher implements DateMatcher {
// 
//    private static Pattern DATE_PATTERN = Pattern.compile(
//      "^\\d{4}-\\d{2}-\\d{2}$");
// 
//    @Override
//    public boolean matches(String date) {
//        return DATE_PATTERN.matcher(date).matches();
//    }
//}

public class TermFrequency extends Configured implements Tool {
	
    public static String stripAccents(String input){
        return input == null ? null :
                Normalizer.normalize(input, Normalizer.Form.NFD)
                        .replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
    }
    

	public static class Map extends
			Mapper<Object, Text, Text, DoubleWritable> {

//		private tokenfrequencyStruct docFrequency = new tokenfrequencyStruct();
		private final static DoubleWritable one  = new DoubleWritable( 1);
//		private Text token = new Text();
		
		List<String> stopwords = Arrays.asList("a", "about", "above", "above", "across", "after", "afterwards", 
				"again", "against", "all", "almost", "alone", "along", "already", "also","although","always",
				"am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone",
				"anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because",
				"become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", 
				"beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call",
				"can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail",
				"do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", 
				"elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything",
				"everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five",
				"for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further",
				"get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter",
				"hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however",
				"hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself",
				"keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me",
				"meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much",
				"must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine",
				"no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off",
				"often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our",
				"ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather",
				"re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should",
				"show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something",
				"sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that",
				"the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby",
				"therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those",
				"though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", 
				"toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
				"very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever",
				"where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", 
				"which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", 
				"within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the",
				 "[", "]", "[ ]","a","b","c","d","e","f","g","h","i","j","k","l","m", "n","o","p","q","r","s",
				 "t","u","v","w","x","y","z","A","B","C","D","E","F","G","H","I","J","K"
				,"L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z", "1", "2", "3", "4", "5", "6",
				"7", "8", "9", "0", "10"
				);


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			Text tokenFileName  = new Text();
	        String token_fileNameString = "";

//			String valString = value.toString().replaceAll("[^a-zA-Z0-9]+"," ");
			String fileterd = value.toString().replaceAll("[^ \\nA-Za-zÀ-ÖØ-öø-ÿ]+"," ");
			String stripAccentsFilter = stripAccents(fileterd);
			
			
			
//			String dateString = value.
//			PorterStemmer stemmer = new PorterStemmer();

			StringTokenizer itr = new StringTokenizer(stripAccentsFilter);
			FileSplit InputfileSplit = (FileSplit) context.getInputSplit();
			String fileName = InputfileSplit.getPath().getName();
			
			while (itr.hasMoreTokens()) {
				String word = itr.nextToken();
				 if (word.isEmpty()) {
		               continue;
		            }
				 
				if (!stopwords.contains(word)) {
//					docFrequency.set(fileName, 1);
					
					token_fileNameString = word.toLowerCase()+"#####" + fileName;
		           	tokenFileName = new Text(token_fileNameString);
		           	
					context.write(tokenFileName, one);}
			}
		}
	}
	
	  	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable > {
		      @Override 
		      public void reduce(Text word, Iterable<DoubleWritable> occurences, Context context) throws IOException, InterruptedException {
		         double sum  = 0.0;  
		         Text delimitedToken = new Text();
//		         System.out.println("occurences " + occurences);
		         
		         for (DoubleWritable count : occurences) {
		            sum  += count.get();
		         }
		         // Normalized term frequency: https://www.cl.cam.ac.uk/teaching/1314/InfoRtrv/lecture4.pdf see page 22
		         sum = 1 + Math.log10(sum);
//		         System.out.println("inside reduce    " + word + " " +  sum);
		         
		         String temp1 =  word.toString() + "#####" ;
		         delimitedToken = new Text(temp1);
		         
		         context.write(delimitedToken, new DoubleWritable(sum));        
		      }
		   }
	


	public int run(String[] args) throws Exception {
//		if (args.length != 2) {
//			System.err.println("Usage: TermFrequency, Please pass correct input args lalalallal");
//			System.exit(2);
//		}
		
		Job job = Job.getInstance(getConf(), " TermFrequency ");
		
		job.setJarByClass(this.getClass());
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		int res  = ToolRunner .run( new TermFrequency(), args);
		System.exit(res);
	}
	
	
}
