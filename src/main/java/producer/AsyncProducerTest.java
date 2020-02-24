package producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * 发送异步消息
 */
public class AsyncProducerTest {
    public static void main(String[] args) throws Exception {
        //1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定Nameserver地址
        producer.setNamesrvAddr("192.168.52.139:9876;192.168.52.140:9876");
        //3.启动producer
        producer.start();
        for (int i = 0; i < 10; i++) {
            //4.创建消息对象，指定主题Topic、Tag和消息体
            /**
             * 参数1：消息主题Topic
             * 参数2：消息Tag
             * 参数3：消息内容
             */
            Message msg = new Message("Animals", "cat", ("cat_ " + i).getBytes());
            //5.发送消息
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult result) {
                    //发送状态
                    SendStatus staus = result.getSendStatus();
                    //消息ID
                    String msgId = result.getMsgId();
                    //消息接收队列ID
                    int queueId = result.getMessageQueue().getQueueId();
                    System.out.println("发送状态：" + staus + ",消息ID：" + msgId + ",队列:" + queueId);

                    System.out.println("发送结果：" + result);

                }

                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                }
            });
            //线程睡一秒
            TimeUnit.MILLISECONDS.sleep(100);
        }
        //6.关闭生产者producer
        producer.shutdown();
    }
}
