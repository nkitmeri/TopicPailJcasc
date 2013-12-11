package data.pail.topicpail;

import data.pail.app.ThriftPailStructure;
import data.thrift.topicthrift.Topic;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author nikos
 */
public class TopicPailStructure extends ThriftPailStructure<Topic>
{
    @Override
    public Class getType()
    {
        return Topic.class;
    }
    
    @Override
    protected Topic createThriftObject() 
    {
        return new Topic();
    }
    
    @Override
    public List< String > getTarget( Topic object )
    {
        return Collections.EMPTY_LIST;
    }
    
    @Override
    public boolean isValidTarget( String... dirs )
    {
        return true;
    }
}
