参考 https://github.com/lpxxn/go-utils
简易 异步队列
利用redis列表的特点创建简单的异步队列，生产者负责把数据push到list中，在消费端从list中获取数据执行业务相关逻辑操作。
