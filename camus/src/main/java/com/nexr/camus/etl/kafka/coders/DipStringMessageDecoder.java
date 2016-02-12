package com.nexr.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

public class DipStringMessageDecoder extends MessageDecoder<Message, String> {

    private static final String DELIMITER = "&";

    private static final Logger log = Logger.getLogger(DipStringMessageDecoder.class);

    @Override
    public void init(Properties props, String topicName) {
        this.props = props;
        this.topicName = topicName;

    }

    @Override
    public CamusWrapper<String> decode(Message message) {

        long timestamp = 0;
        String payloadString;

        try {
            payloadString = new String(message.getPayload(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("Unable to load UTF-8 encoding, falling back to system default", e);
            payloadString = new String(message.getPayload());
        }
        if (payloadString.indexOf(DELIMITER, 0) != -1) {
            int index = payloadString.indexOf(DELIMITER, 0);
            String timeStrResult = payloadString.substring(0, index);
            timestamp = getTime(timeStrResult);
            String m = payloadString.substring(index + 1, payloadString.length());
            payloadString = m;
        } else {
            timestamp = System.currentTimeMillis();
        }

        return new CamusWrapper<String>(payloadString, timestamp);
    }

    private long getTime(String timestampStr) {
        try {
            return Long.valueOf(timestampStr);
        } catch (Exception e) {
            return System.currentTimeMillis();
        }

    }
}