package consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * 广播消费
 */
public class BroadcastConsumerTest {
    public static void main(String[] args) throws Exception {
        //1.创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer comsumer = new DefaultMQPushConsumer("group1");
        //2.指定Nameserver地址
        comsumer.setNamesrvAddr("192.168.52.139:9876;192.168.52.140:9876");
        //3.订阅主题Topic和Tag
        comsumer.subscribe("base", "Tag1");
        //广播模式消费
        comsumer.setMessageModel(MessageModel.BROADCASTING);
        //4.设置回调函数，处理消息
        comsumer.registerMessageListener(new MessageListenerConcurrently() {
            //接受消息内容
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //5.启动消费者consumer
        comsumer.start();
    }
}
