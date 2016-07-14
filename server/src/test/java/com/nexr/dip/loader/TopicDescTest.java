package com.nexr.dip.loader;

import org.junit.Test;

public class TopicDescTest {

    @Test
    public void test() {
        TopicManager.TopicDesc desc = new TopicManager.TopicDesc("name", "path", Loader.SrcType.avro );
        TopicManager topicManager = new TopicManager(desc, null, 0l);
        System.out.println(topicManager.getTopicDesc().getAppPath());
    }

    @Test
    public void testClass() {
        Class<TopicManager.TopicDesc> topicDescClass1 = TopicManager.TopicDesc.class;
        System.out.println("class 1 : " + topicDescClass1);

        TopicManager.TopicDesc desc = new TopicManager.TopicDesc("name", "path", Loader.SrcType.avro );
        Class<TopicManager.TopicDesc> topicDescClass2 = (Class<TopicManager.TopicDesc>) desc.getClass();
        System.out.println("class 2 : " + topicDescClass2);

        try {
            Class topicDescClass3 = Class.forName("com.nexr.dip.loader.TopicManager$TopicDesc");
            System.out.println("class 3 : " + topicDescClass3);
        }catch (Exception e) {
            e.printStackTrace();
        }


    }

}
