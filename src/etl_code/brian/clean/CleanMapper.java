import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int MISSING = 9999;
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");



        if (split.length >= 18 && Integer.parseInt(split[15]) > 1000){ // Check if there are more than 1000 votes
            if(!split[16].equals("Null") && !split[18].equals("Null")) { // Ensure it has data on budget and worldwide income

                String budget_currency = ""; // Determine currency of budget, will create a new column with currency
                if (split[16].startsWith("$")){
                    budget_currency = "USD";
                    split[16] = split[16].substring(1).trim();
                } else {
                    String[] budget_split = split[16].split(" ");
                    budget_currency = budget_split[0];
                    split[16] = budget_split[1].trim();
                }

                if (split[18].startsWith("$")){ // Check if worldwide income is in USD, if so, trim off $ and extra spaces
                    split[18] = split[18].substring(1).trim();

                    int month = 1;
                    String[] datesplit = split[4].split("-");
                    if (datesplit.length==3){ // Get month from date, otherwise default to 1 (January)
                        month = Integer.parseInt(datesplit[1]);
                    }
                    context.write(new Text(split[1]+","+split[3]+ "," + Integer.toString(month)+","+split[5]+","+split[6]+","+split[7]+","+split[14]
                    +","+split[15]+","+split[16]+","+split[18] + "," + budget_currency), new Text(""));
                }
                
            } 
        }
    }
}
