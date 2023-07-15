package github.com.tcw.ibsen.client;

import com.google.protobuf.ByteString;
import github.com.tcw.ibsen.client.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.stream.Collectors;

public class IbsenClient {

    private final IbsenGrpc.IbsenBlockingStub blockingStub;
    private final IbsenGrpc.IbsenStub asyncStub;

    public IbsenClient(String host, int port) {
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        blockingStub = IbsenGrpc.newBlockingStub(channel);
        asyncStub = IbsenGrpc.newStub(channel);
    }

    public List<String> topics() {
        TopicList list = blockingStub.list(EmptyArgs.newBuilder().build());
        return list.getTopicsList().asByteStringList().stream()
                .map(ByteString::toStringUtf8).collect(Collectors.toList());
    }

    public long write(String topic, List<ByteString> entries) {
        InputEntries inputEntries = InputEntries.newBuilder().setTopic(topic).addAllEntries(entries).build();
        WriteStatus writeStatus = blockingStub.write(inputEntries);
        return writeStatus.getWrote();
    }

    public void read(String topic, StreamObserver<OutputEntries> observer) {
        read(topic, 0, 1000, observer);
    }

    public void read(String topic, long offset, StreamObserver<OutputEntries> observer) {
        read(topic, offset, 1000, observer);
    }

    public void read(String topic, long offset, int batchSize, StreamObserver<OutputEntries> observer) {
        ReadParams readParams = ReadParams.newBuilder().setTopic(topic)
                .setOffset(offset)
                .setBatchSize(batchSize)
                .build();

        asyncStub.read(readParams, observer);
    }
}
