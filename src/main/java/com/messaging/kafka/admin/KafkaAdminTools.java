package com.messaging.kafka.admin;

import com.messaging.kafka.admin.api.IAdminSite;
import com.messaging.kafka.admin.api.ITopicBatchBuild;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

class KafkaAdminTools implements IAdminSite {
    Properties config = new Properties();
    AdminClient admin ;
    StringBuilder builder=new StringBuilder();
    KafkaAdminTools()
    {

    }

    CreateTopicsResult createTopicsBase(Collection<NewTopic> topics)
    {

       return admin.createTopics(topics);

    }
    ListTopicsResult listTopics()
    {
        return admin.listTopics();
    }

    public ITopicBatchBuild withTopicBatchBuilder()
    {
        if(admin==null)
            return null;
        return new ITopicBatchBuild()
        {
            Set<NewTopic> collection= new HashSet<NewTopic>();

            public ITopicBatchBuild add(String name, int partitions, short replications) {
                collection.add(new NewTopic(name,partitions,(short)replications));
                return this;
            }

            public KafkaFuture commit() {
                try {
                    return createTopicsBase(collection).all();
                }
                finally
                {
                    collection.clear();
                }


            }
        };

    }

    public KafkaFuture<Set<String>> availableTopicNames() {
        if(admin==null)
            return null;
        return admin.listTopics().names();
    }

    public IAdminSite with(Properties props) {
        if(props.containsKey("url_port"))
        {
            if(builder.length()>0)
                builder.append(',');
            builder.append(props.get("url_port"));
            props.remove("url_port");

        }
        config.putAll(props);
        return this;
    }

    public IAdminSite withUrlAndPort(String url, int port) {
        if(builder.length()>0)
            builder.append(',');
        builder.append(url).append(':').append(port);
        return this;
    }



    public IAdminSite construct() {



        if(admin==null) {
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, builder.toString());
            admin = AdminClient.create(config);
        }
        return this;
    }


}
