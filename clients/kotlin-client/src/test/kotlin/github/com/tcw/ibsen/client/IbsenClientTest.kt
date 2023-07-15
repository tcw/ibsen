package github.com.tcw.ibsen.client

import com.google.protobuf.ByteString.copyFromUtf8
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.function.Consumer


@Testcontainers
class IbsenClientTest {

    lateinit var ibsenClient: IbsenClient

    @Container
    var ibsenContainer: GenericContainer<*> = GenericContainer(DockerImageName.parse("ibsen"))
        .withExposedPorts(50001)
        .withLogConsumer(Consumer { outputFrame: OutputFrame -> print(outputFrame.utf8String) })
        .waitingFor(LogMessageWaitStrategy().withRegEx(".*Started ibsen server.*"))

    @BeforeEach
    fun setUp() {
        val address = ibsenContainer.host
        val port = ibsenContainer.getMappedPort(50001)
        ibsenClient = IbsenClient(address,port)
    }

    @Test
    fun writeEntriesToTopic() {
        val totalBytesWritten: Long = ibsenClient.write(
            "test",
            listOf(copyFromUtf8("entry1"), copyFromUtf8("entry2"), copyFromUtf8("entry3"))
        )
        Assertions.assertEquals(3, totalBytesWritten)
    }
}