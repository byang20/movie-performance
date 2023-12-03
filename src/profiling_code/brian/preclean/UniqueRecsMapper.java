import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueRecsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int MISSING = 9999;
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        context.write(new Text("Title"), new Text(split[1]));
        context.write(new Text("Year"), new Text(split[3]));
        context.write(new Text("Rating"), new Text(split[14]));

    }
}


