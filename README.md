参考：https://help.aliyun.com/document_detail/43490.html?spm=a2c4g.11186623.6.583.veJfUd

MQ 消费者在接收到消息以后，有必要根据业务上的唯一 Key 对消息做幂等处理的必要性。

消费幂等的必要性
在互联网应用中，尤其在网络不稳定的情况下，MQ 的消息有可能会出现重复，这个重复简单可以概括为以下两种情况：

发送时消息重复：

当一条消息已被成功发送到服务端并完成持久化，此时出现了网络闪断或者客户端宕机，导致服务端对客户端应答失败。 如果此时 MQ Producer 意识到消息发送失败并尝试再次发送消息，MQ 消费者后续会收到两条内容相同并且 Message ID 也相同的消息。

投递时消息重复：

MQ Consumer 消费消息场景下，消息已投递到消费者并完成业务处理，当客户端给服务端反馈应答的时候网络闪断。 为了保证消息至少被消费一次，MQ 服务端将在网络恢复后再次尝试投递之前已被处理过的消息，MQ 消费者后续会收到两条内容相同并且 Message ID 也相同的消息。

处理建议
因为 Message ID 有可能出现冲突（重复）的情况，所以真正安全的幂等处理，不建议以 Message ID 作为处理依据。 最好的方式是以业务唯一标识作为幂等处理的关键依据，而业务的唯一标识可以通过消息 Key 进行设置:

Message message = new Message();
message.setKey("ORDERID_100");
SendResult sendResult = producer.send(message);
订阅方收到消息时可以根据消息的 Key 进行幂等处理：

consumer.subscribe("ons_test", "*", new MessageListener() {
    public Action consume(Message message, ConsumeContext context) {
        String key = message.getKey()
        // 根据业务唯一标识的 key 做幂等处理
    }
});