import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StockMapper
        extends Mapper<LongWritable, Text, Text, LongWritable> {
   
    private static final int MISSING = 9999;
    private static final String START_DATE = "3/2/20";
    private static final String END_DATE = "3/26/21";
    private boolean isStarted = false;

    private int numDaysByWeek = 0;
    private String currentDateIndicator = "3/26/21";

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString();
        String[] words = line.split("\t");
        
        if (words.length == 1) {
	    words = line.split("  ");
	}

        String date = words[0];
        int dailyGrowth = Integer.parseInt(words[1]);
        
        if (START_DATE.equals(date)) {
	    isStarted = true;
	}

        if (END_DATE.equals(date)) {
	    isStarted = false;
	}

        if (!isStarted) {
	    return;
	}
        
        if (numDaysByWeek == 0) currentDateIndicator = date;
        

        context.write(new Text(currentDateIndicator), new LongWritable(dailyGrowth));
        
        numDaysByWeek++;
        numDaysByWeek %= 7;
    }
}
