import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class InvertedIndexJob 
{

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text DocumentIdTextVar = new Text();
		private Text WordTextVar = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String Line = value.toString();
			StringTokenizer ItrVar = new StringTokenizer(Line, " \t\n\r\f,.!/-?:;@_#()\"");

			
	
			if (ItrVar.hasMoreTokens())
			{

				DocumentIdTextVar.set(ItrVar.nextToken());
			
			}
				
	
			do  	
			{
				String word = ItrVar.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
				if(word.length()>0) 
				{
					WordTextVar.set(word);
					context.write(WordTextVar, DocumentIdTextVar);
				}
			}	while(ItrVar.hasMoreTokens());
		}
	}


	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> 
	{

		private Text ResultText = new Text();

		@Override
		//creating map
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Map<String, Integer> map = new HashMap<>();

			
			for (Text TextVar : values) 
			{

				if(TextVar.toString().contains(":")) {

									String mapText = TextVar.toString();

					for(String FromHere : mapText.split(" ")) {
						
						int ColonIndex = FromHere.indexOf(":");
						
						String ThedocIdVal = FromHere.substring(0, ColonIndex);
						//take a counter
									int Counter = Integer.parseInt(FromHere.substring(ColonIndex + 1).trim());

						if (map.containsKey(ThedocIdVal)) 
						{
							//int i=0;
							int CounterOne = map.get(ThedocIdVal);

						map.put(ThedocIdVal,  CounterOne+Counter);
	
						} else 
						{
							map.put(ThedocIdVal, Counter);
						}
						
					}
				}
				else 
				{
					// for(int i=0;i<10;i++)
					// {
					// 	gahghkabsjkahjksn
					// }
					String DocumentIdTextVarVal = TextVar.toString();					
						if (map.containsKey(DocumentIdTextVarVal)) 
						{	
							int Counter = map.get(DocumentIdTextVarVal);
							//int i=0;
							map.put(DocumentIdTextVarVal , 1+Counter);	
						} 
						else 
						{
							//put to map
							map.put(DocumentIdTextVarVal, 1);
						}	
				}
			}


			StringBuilder CountList = new StringBuilder();
			for (Entry<String, Integer> a : map.entrySet()) 
			{			
				String docIdValue = a.getKey();

				int Count = a.getValue(); 
				//int i=0;

				CountList.append(String.format("%s:%d ", docIdValue, Count));
			}
			//write
			ResultText.set(CountList.toString());

				context.write(key, ResultText);
		}
	}

	public static void main(String[] args) throws Exception {

		// if(args.length!=2)
		// {
		// 	System.err.println("Usage: Word Count <input path> <output path>");
		// 	System.exit(-1);
		// }

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");

		job.setJarByClass(InvertedIndexJob.class);
		job.setMapperClass(TokenizerMapper.class);
		//Combine
		job.setCombinerClass(InvertedIndexReducer.class);
		//check
		job.setReducerClass(InvertedIndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}