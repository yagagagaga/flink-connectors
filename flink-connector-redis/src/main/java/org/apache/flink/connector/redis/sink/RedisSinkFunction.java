package org.apache.flink.connector.redis.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.LinkedList;

/**
 * RedisSinkFunction for Redis multi sentinel cluster.
 */
public class RedisSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private final RedisOptions redisOptions;
    private final RedisRecordConsumer<T> consumer;
    private final TypeInformation<T> typeInfo;

    private final LinkedList<T> buffer = new LinkedList<>();
    private transient ListState<T> bufferState;

    private transient Sender<T> sender;

    public RedisSinkFunction(TypeInformation<T> typeInfo, RedisRecordConsumer<T> consumer, RedisOptions redisOptions) {
        this.typeInfo = typeInfo;
        this.consumer = consumer;
        this.redisOptions = redisOptions;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sender = Sender.getOrCreate(consumer, redisOptions.createClient(), redisOptions);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (sender == null) {
            return;
        }
        buffer.offer(value);
        if (buffer.size() > redisOptions.batchSize()) {

            bufferState.clear();
            bufferState.addAll(buffer);

            buffer.forEach(sender::send);

            buffer.clear();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        bufferState.clear();
        bufferState.addAll(buffer);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        bufferState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("shardedJedisSinkState", typeInfo));
        if (context.isRestored()) {
            bufferState.get().forEach(buffer::add);
        }
    }
}
