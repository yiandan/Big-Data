import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Double;

public class CleanMapper
    extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final int MISSING = 0;
    private static final int DATE_INDEX = 0;
    private static final int PerChangeMarketCap_INDEX = 4;

    @Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(",");
        long change_market_cap = 0;

        try {
            change_market_cap = (long) Double.parseDouble(words[PerChangeMarketCap_INDEX]);
        }
        catch(NumberFormatException e) { 
            change_market_cap = MISSING;
        }
        
        // ignore the header
        if (!"Date".equals(words[DATE_INDEX])) {
            String date = words[DATE_INDEX];
            // put the year at the front, so sorting would be correct
            date = date.substring(date.length() - 2) + "/" + date.substring(0, date.length() - 3);

            // use two digits to represent a month (to solve the sorting problem)
            if (date.substring(3).indexOf("/") == 1) {
                date = date.substring(0, 3) + "0" + date.substring(3);
            }

            // use two digits to represent a date (to solve the sorting problem)
            if (date.charAt(date.length() - 2) == '/') {
                date = date.substring(0, 6) + "0" + date.substring(6);
            }
            context.write(new Text(date), new LongWritable(change_market_cap));
        }
            
    }
}
