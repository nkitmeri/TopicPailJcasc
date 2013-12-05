package data.pail.topicpail;

import data.thrift.topicthrift.Topic;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author nikos
 */
public class TopicPailStructure extends ThriftPailStructure<Topic>
{
    public Class getType()
    {
        return Topic.class;
    }
    
    @Override
    protected Topic createThriftObject() 
    {
        return new Topic();
    }
    
    public List< String > getTarget( Topic object )
    {
        return Collections.EMPTY_LIST;
    }
    
    public boolean isValidTarget( String... dirs )
    {
        return true;
    }
}
