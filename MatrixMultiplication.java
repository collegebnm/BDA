/*Develop a MapReduce program to implement Matrix Multiplication.*/
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.StringBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

  public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      String matrixName = tokenizer.nextToken();
      String rowIndex = tokenizer.nextToken();
      String colIndex = tokenizer.nextToken();
      String elementValue = tokenizer.nextToken();

      if(matrixName.equals("A")){
        for(int i=0; i < 2; i++){
          Text emitKey = new Text(rowIndex + "," + i);
          context.write(emitKey, new Text(matrixName + "," + colIndex + "," + elementValue));
        }
      } else{
        for(int i=0; i < 2; i++){
          Text emitKey = new Text(i + "," + colIndex);
          context.write(emitKey, new Text(matrixName + "," + rowIndex + "," + elementValue));
        }
      }
    }
  }

  public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int result = 0;
      Text resultKey = new Text();
      Text resultValue = new Text();

      String[] valArray = new String[2];
      int[] aElements = new int[2];
      int[] bElements = new int[2];

      for(Text value : values){
        String[] tokens = value.toString().split(",");
        String matrixName = tokens[0];
        int index = Integer.parseInt(tokens[1]);
        int elementValue = Integer.parseInt(tokens[2]);

        if(matrixName.equals("A")){
          aElements[index] = elementValue;
        } else{
          bElements[index] = elementValue;
        }
      }

      for(int i = 0; i < 2; i++){
        result += aElements[i] * bElements[i];
      }

      StringTokenizer tok = new StringTokenizer(key.toString(), ",");
      int rowNum = Integer.parseInt(tok.nextToken());
      int colNum = Integer.parseInt(tok.nextToken());

      resultKey.set(rowNum + "," + colNum);
      resultValue.set(String.valueOf(result));

      context.write(resultKey, resultValue);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MatrixMultiplication");

    job.setJarByClass(MatrixMultiplication.class);
    job.setMapperClass(MatrixMapper.class);
    job.setReducerClass(MatrixReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
