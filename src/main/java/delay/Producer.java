package delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

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
            Message msg = new Message("DelayTopic", "Tag1", ("Delay Test" + i).getBytes());
            // 延迟级别：1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
            msg.setDelayTimeLevel(2);
            //5.发送消息
            SendResult sendResult = producer.send(msg);
            System.out.println("发送状态：" + sendResult);
            //沉睡0.1秒
            TimeUnit.MILLISECONDS.sleep(100);
        }
        //6.关闭生产者
        producer.shutdown();

    }
}
