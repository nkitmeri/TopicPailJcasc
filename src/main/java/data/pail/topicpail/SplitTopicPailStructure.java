package data.pail.topicpail;

import data.thrift.topicthrift.Topic;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author nikos
 */
public class SplitTopicPailStructure extends TopicPailStructure
{
    @Override
    public List<String> getTarget( Topic object )
    {
        ArrayList<String> directoryPath = new ArrayList<>();
        directoryPath.add( "" + ( ( !object.isIsTrend() ) ? 0 : 1 ) );
        
        return directoryPath;
    }
    
    @Override
    public boolean isValidTarget( String... strings )
    {
        if( strings.length != 2 ) return false;
        
        try {
            return strings[0] != null; 
        } catch (Exception e) {
            return false;
        }
    }
}
