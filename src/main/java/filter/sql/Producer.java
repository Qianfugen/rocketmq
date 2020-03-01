package filter.sql;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * 消息发送者：Sql过滤
 */
public class Producer {
    public static void main(String[] args) throws Exception {
        //1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.制定Nameserver地址
        producer.setNamesrvAddr("192.168.52.139:9876;192.168.52.140:9876");
        //3.启动producer
        producer.start();

        for (int i = 0; i < 10; i++) {
            //4.创建消息
            Message msg = new Message("FilterSqlTopic", "Tag1", ("Hello World_" + i).getBytes());
            //设置属性
            msg.putUserProperty("i", String.valueOf(i));
            //5.发送消息
            SendResult sendResult = producer.send(msg);
            System.out.println(sendResult);
            //沉睡0.1秒
            TimeUnit.MILLISECONDS.sleep(100);
        }
        //6.关闭producer
        producer.shutdown();
    }
}
