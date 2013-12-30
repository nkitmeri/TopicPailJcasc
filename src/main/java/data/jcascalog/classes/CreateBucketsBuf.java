package data.jcascalog.classes;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;

/**
 *
 * @author nikos
 */
public class CreateBucketsBuf extends CascalogBuffer {

    private static final DateFormat DATEFORMAT =
                    new SimpleDateFormat( "EEE MMM dd HH:mm:ss ZZZZZ yyyy" );
    private static long firstDate;
    private String date = "";
    private HashMap< Long, Integer > buckets;
    
    static
    {
        try 
        {
            firstDate = DATEFORMAT.parse( "Sat Nov 09 11:46:40 EET 2013" )
                    .getTime();
        } 
        catch( ParseException e )
        {
            throw new RuntimeException(e);
        }
    }
    
    private long getTime( String date )
    {
        try 
        {
                return DATEFORMAT.parse( date )
                    .getTime();
        } 
        catch( ParseException e )
        {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void operate(FlowProcess proccess, BufferCall call) {
        buckets = new HashMap<>();
        Iterator<TupleEntry> iter = call.getArgumentsIterator();
        
        while(iter.hasNext()) 
        {
            TupleEntry t = iter.next();
            date = t.getTuple().toString();
            long poss = ( getTime( date ) - firstDate );
            
            if( buckets.containsKey( poss ) )
            {
                buckets.put( poss, buckets.get( poss ) + 1 );
            }
            else
            {
                buckets.put( poss, 1 );
            }
        }
        
        call.getOutputCollector().add(new Tuple(buckets));
    }
    
}
