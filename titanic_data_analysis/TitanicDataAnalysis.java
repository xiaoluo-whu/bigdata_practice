import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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

public class TitanicDataAnalysis {

    public static class LineMapper
        extends Mapper<Object, Text, Text, FloatWritable>{

        private Text gender = new Text();
        private FloatWritable age = new FloatWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            List<String> elems = Arrays.asList(text.split(","));

            if (StringUtils.isNotEmpty(elems.get(4)) &&
            StringUtils.isNotEmpty(elems.get(5))) {
                gender.set(elems.get(4));
                age.set(Float.parseFloat(elems.get(5)));
                context.write(gender, age);
            }



        }
    }

    public static class AgeRespectiveReducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {
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
        Job job = Job.getInstance(conf, "TitanicDataAnalysis");
        job.setJarByClass(TitanicDataAnalysis.class);
        job.setMapperClass(LineMapper.class);
        job.setReducerClass(AgeRespectiveReducer.class);
        job.setCombinerClass(AgeRespectiveReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
