package batch;

import config.Constants;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) throws Exception {
        //1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.制定Nameserver地址
        producer.setNamesrvAddr(Constants.NAMESRV);
        //3.启动producer
        producer.start();


        //4.创建消息
        List<Message> msgs = new ArrayList<>();
        Message msg1 = new Message("BatchTopic", "Tag1", ("Delay Test" + 1).getBytes());
        Message msg2 = new Message("BatchTopic", "Tag1", ("Delay Test" + 2).getBytes());
        Message msg3 = new Message("BatchTopic", "Tag1", ("Delay Test" + 3).getBytes());
        Message msg4 = new Message("BatchTopic", "Tag1", ("Delay Test" + 4).getBytes());
        Message msg5 = new Message("BatchTopic", "Tag1", ("Delay Test" + 5).getBytes());
        msgs.add(msg1);
        msgs.add(msg2);
        msgs.add(msg3);
        msgs.add(msg4);
        msgs.add(msg5);

        //5.发送消息
        SendResult sendResult = producer.send(msgs);
        System.out.println("发送状态：" + sendResult);
        //沉睡0.1秒
        TimeUnit.MILLISECONDS.sleep(100);
        //6.关闭生产者
        producer.shutdown();

    }
}
