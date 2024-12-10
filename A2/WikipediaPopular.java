import java.util.StringTokenizer;
import java.io.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.regex.Pattern;


public class WikipediaPopular extends Configured implements Tool {

    // We take in LongWritable (byte offset) and Text (file) as input
    // (0, '20160801-020000 en Aaaah 20 231818')
    public static class WikipediaMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private final static IntWritable requests = new IntWritable();
        private Text date = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(" ");
            String dateString = data[0];
            String language = data[1];
            String title = data[2];
            
            if (title.startsWith("Special:") || title.equals("Main_Page") || !language.equals("en")) {
                return;
            }
            requests.set(Integer.parseInt(data[3]));
            date.set(dateString);
            context.write(date, requests);
        }
    }
    // Input is ("2016-08-01-100", 20)
    public static class WikipediaReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final static Text date = new Text();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable max = new IntWritable(0);
            date.set(key);
            for (IntWritable val: values){
                if (val.get() > max.get()) {
                    max.set(val.get());
                }
            }
            context.write(date, max);
        }
    }
        
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
        System.exit(res);
    }
        
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "word count improved");
        job.setJarByClass(WikipediaPopular.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WikipediaMapper.class);
        job.setReducerClass(WikipediaReducer.class);

        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}