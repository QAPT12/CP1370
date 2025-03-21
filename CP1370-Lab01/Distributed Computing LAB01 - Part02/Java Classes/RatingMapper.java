import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RatingMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text rating = new Text();

    public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length >= 3) {
            rating.set(parts[2]); // Extract the rating (third field)
            output.collect(rating, one);
        }
    }
}
