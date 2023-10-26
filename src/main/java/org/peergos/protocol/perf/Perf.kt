package org.peergos.protocol.perf

import io.libp2p.core.BadPeerException
import io.libp2p.core.ConnectionClosedException
import io.libp2p.core.Libp2pException
import io.libp2p.core.Stream
import io.libp2p.core.multistream.StrictProtocolBinding
import io.libp2p.etc.types.completedExceptionally
import io.libp2p.etc.types.lazyVar
import io.libp2p.etc.types.toByteArray
import io.libp2p.protocol.ProtocolHandler
import io.libp2p.protocol.ProtocolMessageHandler
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageCodec
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


/*
Attempt to implement: https://github.com/libp2p/test-plans/tree/master/perf#libp2p-performance-benchmarking

still to add the following every second...
            double latency = (currentMillisecond - startMillisecond) / 1000.0;
            System.out.println("{\"type\": \"intermediary\",\"timeSeconds\": " + latency +
                    ",\"uploadBytes\": " + totalUploadSize
                    + ",\"downloadBytes\": " + total + "}");
 */
interface PerfController {
    fun perf(): CompletableFuture<Long>
}

class Perf(uploadSize: Int, downloadSize: Int) : PerfBinding(PerfProtocol(uploadSize, downloadSize))

open class PerfBinding(perf: PerfProtocol) :
    StrictProtocolBinding<PerfController>("/perf/0.1.0", perf)

class PerfTimeoutException : Libp2pException()

open class PerfProtocol(var uploadSize: Int, var downloadSize: Int) : ProtocolHandler<PerfController>(Long.MAX_VALUE, Long.MAX_VALUE) {
    var timeoutScheduler by lazyVar { Executors.newSingleThreadScheduledExecutor() }
    var curTime: () -> Long = { System.currentTimeMillis() }
    var random = Random()
    var perfTimeout = Duration.ofHours(10) //kev
    var totalUploadSize = uploadSize + 4
    var totalDownloadSize = downloadSize + 4
    override fun onStartInitiator(stream: Stream): CompletableFuture<PerfController> {
        val handler = PerfInitiator()
        stream.pushHandler(PerfCodec())
        stream.pushHandler(handler)
        stream.pushHandler(PerfCodec())
        return handler.activeFuture
    }

    override fun onStartResponder(stream: Stream): CompletableFuture<PerfController> {
        val handler = PerfResponder()
        stream.pushHandler(PerfCodec())
        stream.pushHandler(handler)
        stream.pushHandler(PerfCodec())
        return CompletableFuture.completedFuture(handler)
    }

    open class PerfCodec : ByteToMessageCodec<ByteArray>() {
        override fun encode(ctx: ChannelHandlerContext?, msg: ByteArray, out: ByteBuf) {
            out.writeInt(msg.size)
            out.writeBytes(msg)
        }

        override fun decode(ctx: ChannelHandlerContext?, msg: ByteBuf, out: MutableList<Any>) {
            val readerIndex = msg.readerIndex()
            if (msg.readableBytes() < 4) {
                return
            }
            val len = msg.readInt()
            val readable = msg.readableBytes()
            if (readable < len) {
                // not enough data to read the full array
                // will wait for more ...
                msg.readerIndex(readerIndex)
                return
            }
            val data = msg.readSlice(len)
            out.add(data.toByteArray())
        }
    }

    open inner class PerfResponder : ProtocolMessageHandler<ByteArray>, PerfController {
        override fun onMessage(stream: Stream, msg: ByteArray) {
            val arr = ByteArray(totalDownloadSize)
            random.nextBytes(arr)
            //copy across correlation id
            for (i in 0..3) {
                arr[i] = msg[i]
            }
            stream.writeAndFlush(arr)
        }

        override fun perf(): CompletableFuture<Long> {
            throw Libp2pException("This is perf responder only")
        }
    }

    open inner class PerfInitiator : ProtocolMessageHandler<ByteArray>, PerfController {
        val activeFuture = CompletableFuture<PerfController>()
        val requests = Collections.synchronizedMap(mutableMapOf<String, Pair<Long, CompletableFuture<Long>>>())
        lateinit var stream: Stream
        var closed = false

        override fun onActivated(stream: Stream) {
            this.stream = stream
            activeFuture.complete(this)
        }

        override fun onMessage(stream: Stream, msg: ByteArray) {
            val dataS = "" + Correlation.id(msg)
            val (sentT, future) = requests.remove(dataS)
                ?: throw BadPeerException("Unknown or expired perf data in response: $dataS")
            future.complete((curTime() - sentT))
        }

        override fun onClosed(stream: Stream) {
            synchronized(requests) {
                closed = true
                requests.values.forEach { it.second.completeExceptionally(ConnectionClosedException()) }
                requests.clear()
                timeoutScheduler.shutdownNow()
            }
            activeFuture.completeExceptionally(ConnectionClosedException())
        }

        override fun perf(): CompletableFuture<Long> {
            val ret = CompletableFuture<Long>()
            val arr = ByteArray(totalUploadSize)
            random.nextBytes(arr)
            val start = curTime()
            val dataS = "" + Correlation.id(arr)
            synchronized(requests) {
                if (closed) return completedExceptionally(ConnectionClosedException())
                requests[dataS] = start to ret

                timeoutScheduler.schedule(
                    {
                        requests.remove(dataS)?.second?.completeExceptionally(PerfTimeoutException())
                    },
                    perfTimeout.toMillis(),
                    TimeUnit.MILLISECONDS
                )
            }
            stream.writeAndFlush(arr)
            return ret
        }
    }
}