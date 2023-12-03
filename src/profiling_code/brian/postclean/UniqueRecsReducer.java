import java.io.IOException;

import java.util.HashMap;

import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueRecsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashMap<String, Integer> dict= new HashMap<>();
        int count = 0;
        for (Text value : values) {
            if (!dict.containsKey(value.toString())){
                dict.put(value.toString(),0);
            }
        }

        context.write(key, new Text(Integer.toString(dict.size()))); 
        
    }
}


