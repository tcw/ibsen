package github.com.tcw.ibsen.client

import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver

class IbsenClient(host: String?, port: Int) {
    private val blockingStub: IbsenGrpc.IbsenBlockingStub
    private val asyncStub: IbsenGrpc.IbsenStub

    init {
        val channelBuilder: ManagedChannelBuilder<*> = ManagedChannelBuilder
            .forAddress(host, port)
            .usePlaintext()
        val channel: ManagedChannel = channelBuilder.build()
        blockingStub = IbsenGrpc.newBlockingStub(channel)
        asyncStub = IbsenGrpc.newStub(channel)
    }

    fun topics(): List<String> {
        val list = blockingStub.list(EmptyArgs.getDefaultInstance())
        return list.topicsList.asByteStringList().map { it.toStringUtf8() }
    }

    fun write(topic: String?, entries: List<ByteString?>?): Long {
        val inputEntries: InputEntries = InputEntries.newBuilder().setTopic(topic).addAllEntries(entries).build()
        val writeStatus: WriteStatus = blockingStub.write(inputEntries)
        return writeStatus.wrote
    }

    suspend fun read(topic: String?, observer: StreamObserver<OutputEntries?>?) {
        read(topic, 0, 1000, observer)
    }

    suspend fun read(topic: String?, offset: Long, observer: StreamObserver<OutputEntries?>?) {
        read(topic, offset, 1000, observer)
    }

    private suspend fun read(topic: String?, offset: Long, batchSize: Int, observer: StreamObserver<OutputEntries?>?) {
        val readParams: ReadParams = ReadParams.newBuilder().setTopic(topic)
            .setOffset(offset)
            .setBatchSize(batchSize)
            .build()
        asyncStub.read(readParams, observer)
    }
}
