/*Develop a MapReduce to find the maximum electrical consumption in each year given
electrical consumption for each month in each year.*/
import java.io.IOException;
import java.util.StringTokenizer;

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

public class MaxElectricityConsumption {

  public static class MaxElectricityConsumptionMapper extends Mapper<Object, Text, IntWritable, FloatWritable>{

    private IntWritable year = new IntWritable();
    private FloatWritable consumption = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);

      if(tokenizer.hasMoreTokens()) {
        year.set(Integer.parseInt(tokenizer.nextToken()));
      }

      while (tokenizer.hasMoreTokens()) {
        consumption.set(Float.parseFloat(tokenizer.nextToken()));
        context.write(year, consumption);
      }
    }
  }

  public static class MaxElectricityConsumptionReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
      float maxConsumption = Float.MIN_VALUE;

      for (FloatWritable value : values) {
        if (value.get() > maxConsumption) {
          maxConsumption = value.get();
        }
      }

      context.write(key, new FloatWritable(maxConsumption));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "max electricity consumption");

    job.setJarByClass(MaxElectricityConsumption.class);
    job.setMapperClass(MaxElectricityConsumptionMapper.class);
    job.setReducerClass(MaxElectricityConsumptionReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(FloatWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
