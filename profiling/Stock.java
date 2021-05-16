import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Stock {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Stock <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(Stock.class);
        job.setJobName("Stock");
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}