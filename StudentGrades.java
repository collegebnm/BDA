/*Develop a MapReduce program to find the grades of student’s. */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentGrades {

    public static class GradeMapper extends Mapper<Object, Text, Text, Text> {

        private Text studentId = new Text();
        private Text result = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
            String id = tokenizer.nextToken();
            double sum = 0;
            int count = 0;

            while (tokenizer.hasMoreTokens()) {
                sum += Double.parseDouble(tokenizer.nextToken());
                count++;
            }

            double percentage = sum / count;

            studentId.set(id);
            result.set(Double.toString(percentage));
            context.write(studentId, result);
        }
    }

    public static class GradeReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double percentage = 0;
            int count = 0;

            for (Text value : values) {
                percentage = Double.parseDouble(value.toString());
                count++;
            }

            double avgPercentage = percentage / count;
            String grade = getGrade(avgPercentage);

            context.write(key, new Text(grade));
        }

        private String getGrade(double percentage) {
            if (percentage >= 90) {
                return "A";
            } else if (percentage >= 80) {
                return "B";
            } else if (percentage >= 70) {
                return "C";
            } else if (percentage >= 60) {
                return "D";
            } else {
                return "F";
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "student grades");
        job.setJarByClass(StudentGrades.class);
        job.setMapperClass(GradeMapper.class);
        job.setReducerClass(GradeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
