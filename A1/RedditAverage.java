import java.io.IOException;
import java.util.StringTokenizer;
import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

import java.util.regex.Pattern;

public class RedditAverage extends Configured implements Tool {

    // System.out.println((String) record.get("subreddit"));
    // System.out.println((Integer) record.get("score"));

    public static class RedditMapper 
    extends Mapper<LongWritable, Text, Text, LongPairWritable> {
        private final static LongPairWritable pair = new LongPairWritable();
        private Text subreddit = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // record format: (comment, score) -> ("this subreddit sucks!", "35")
            JSONObject record = new JSONObject(value.toString());
            Long score = record.getLong("score");
            String name = record.getString("subreddit");
            subreddit.set(name);
            // pair is (commentCount, totalScore)
            pair.set(1, score);
            context.write(subreddit, pair);
        }
    }    

        public static class RedditCombiner
        extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
            private final static LongPairWritable pair = new LongPairWritable();

            @Override
            public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
                long commentCount = 0;
                long totalScore = 0;
                for (LongPairWritable val : values) {
                    commentCount += val.get_0();
                    totalScore += val.get_1();
                }
                pair.set(commentCount, totalScore);
                context.write(key, pair);
            }
        }

        public static class RedditReducer
        extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
			private DoubleWritable average = new DoubleWritable();

			@Override
			public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
				long commentCount = 0;
                long totalScore = 0;
				for (LongPairWritable val : values) {
                    commentCount += val.get_0();
                    totalScore += val.get_1();
				}
				average.set((double) totalScore/commentCount);
				context.write(key, average);
			}
        }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "reddit average");
        job.setJarByClass(RedditAverage.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(RedditMapper.class);
        job.setCombinerClass(RedditCombiner.class);
        job.setReducerClass(RedditReducer.class);

        job.setMapOutputKeyClass(Text.class); // Map outputs text as key, and LPW as value
        job.setMapOutputValueClass(LongPairWritable.class);
        job.setOutputKeyClass(Text.class); // reducer outputs Text as key, and DW as value
        job.setOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
