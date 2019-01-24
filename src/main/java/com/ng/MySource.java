package com.ng;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

public class MySource extends AbstractSource implements Configurable , PollableSource {

    private String preText;
    private Long delayTime;

    public Status process() throws EventDeliveryException {
        /*
         *1、获取数据  2、封装event  3、写入channel
         */
        //1、使用for循环模拟获取数据
        try {
            for (int i = 0; i < 5; i++) {
                //2、封装事件
                SimpleEvent event = new SimpleEvent();
                event.setHeaders(new HashMap<String, String>());
                event.setBody((preText + i).getBytes());

                //3、写入channel
                getChannelProcessor().processEvent(event);

                Thread.sleep(delayTime);
            }
        }catch(Exception e){
                e.printStackTrace();
                return  Status.BACKOFF;
            }

        return Status.READY;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    public void configure(Context context) {
        preText = context.getString("preText","ng");
        delayTime = context.getLong("delayTime");
    }
}
