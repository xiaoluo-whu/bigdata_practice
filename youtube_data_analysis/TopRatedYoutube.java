import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TopRatedYoutube {

    public static class LineMapper
        extends Mapper<Object, Text, Text, FloatWritable>{

        private Text video_name = new Text();
        private FloatWritable rating = new FloatWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            List<String> elems = Arrays.asList(text.split("\t"));

            if (elems.size() > 7) {
                video_name.set(elems.get(0));
                if (elems.get(6).matches("\\d+.+")) {
                    //this regular expression
                    //specifies that the string should contain only floating values
                    rating.set(Float.parseFloat(elems.get(6)));
                }
            }

            context.write(video_name, rating);
        }
    }

    public static class FloatAvgReducer
            extends Reducer<Text,FloatWritable,Text,FloatWritable> {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0.0f;
            int count = 0;
            for (FloatWritable val : values) {
                count++;
                sum += val.get();
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: TopRatedYoutube <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "TopRatedYoutube");
        job.setJarByClass(TopRatedYoutube.class);
        job.setMapperClass(LineMapper.class);
        job.setCombinerClass(FloatAvgReducer.class);
        job.setReducerClass(FloatAvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
