package data.pail.tweetpail;

import data.thrift.tweetthrift.Tweet;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

/**
 *
 * @author nikos
 */
public class SplitTweetPailStructure extends TweetPailStructure
{
    @Override
    public List<String> getTarget( Tweet object )
    {
        try 
        {
            ArrayList<String> directoryPath = new ArrayList<>();
            String dateStr = object.date.cratedAt;
            DateFormat dateFormat =
                    new SimpleDateFormat( "EEE MMM dd HH:mm:ss ZZZZZ yyyy" );
            Date date = dateFormat.parse( dateStr );
            
            Calendar cal = new GregorianCalendar();
            
            cal.setTime( date );
            
            directoryPath.add( "" + cal.get( Calendar.YEAR ) );
            directoryPath.add( "" + ( cal.get( Calendar.MONTH  ) + 1 ) );
            directoryPath.add( "" + cal.get( Calendar.WEEK_OF_MONTH ) );
            directoryPath.add( "" + cal.get( Calendar.DAY_OF_MONTH ) );

            return directoryPath;
        } 
        catch( ParseException e )
        {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public boolean isValidTarget( String... strings )
    {
        if( strings.length != 5 ) return false;
        
        try {
            return strings[0] != null; 
        } catch (Exception e) {
            return false;
        }
    }
}
