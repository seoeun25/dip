package com.nexr.dip.producer;

import com.nexr.dip.DipException;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sending the message through the producer.
 */
public class Senders {

    public static final long BLOCKING_TIMEOUT = 10000;

    public static enum Sender {
        AsyncSender {
            @Override
            boolean send(KafkaProducer kafkaProducer, final ProducerRecord record, final SenderCallback callback) throws DipException{
                try {
                    kafkaProducer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null) {
                                try {
                                    callback.onFail2(record, exception);
                                } catch (DipException e) {
                                    e.printStackTrace();
                                }
                            } else {
                                callback.onSucceed(metadata);
                            }
                        }
                    });
                    return true;
                } catch (BufferExhaustedException e) {
                    callback.onFail2(record, e);
                    return false;
                }
            }

        },
        BlockingSender {
            @Override
            boolean send(KafkaProducer kafkaProducer, ProducerRecord record, final SenderCallback callback) throws DipException{
                long start = System.currentTimeMillis();
                try {

                    final Future<RecordMetadata> future = kafkaProducer.send(record);
                    RecordMetadata metadata = future.get(BLOCKING_TIMEOUT, TimeUnit.MILLISECONDS);
                    callback.onSucceed(metadata);
                    return true;
                } catch (InterruptedException e) {
                    System.out.println("Passed " + BLOCKING_TIMEOUT);
                    //callback.onFail2(record, e);
                } catch (BufferExhaustedException e) {
                    System.out.println("---- BufferExhaustedException");
                    callback.onFail2(record, e);
                } catch (ExecutionException e) {
                    System.out.println("---- ExecutionException");
                    System.out.println(System.currentTimeMillis() - start);
                    callback.onFail2(record, e);
                } catch (CancellationException e) {
                    System.out.println("---- CancellationException");
                    callback.onFail2(record, e);
                } catch (TimeoutException e) {
                    System.out.println("---- TimeoutException");
                    callback.onFail2(record, e);
                }
                return false;
            }
        };

        abstract boolean send(KafkaProducer kafkaProducer, ProducerRecord record, SenderCallback callback) throws DipException;
    }

    static class SenderCallback {

        public void onFail2(ProducerRecord record, Throwable throwable) throws DipException{
            throw new DipException("SentFailed record : " + record.toString() , throwable);
        }

        public void onSucceed(RecordMetadata metadata) {
//            System.out.println("Send Succeed : record meta topic: [" + metadata.topic() + "],  partition: [" +
//                    metadata.partition() + "] , offset : [" + metadata.offset() + "]");
        }
    }

}
