use futures_lite::future::{block_on, poll_once, zip};
use rivet::{
    Runtime, RuntimeConfig, SendPayload, TcpListener, TcpStream,
    net::{StreamSend, StreamShutdown},
    runtime, time,
};
use std::{future::Future, io, net::Shutdown, time::Duration};

fn runtime() -> Runtime {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_tasks = 8;
    config.limits.max_sockets = 8;
    config.limits.max_operations = 64;
    config.limits.max_pending_accepts = 2;
    config.limits.max_pending_receives = 2;
    config.limits.pool.bytes = 1024 * 1024;
    config.limits.pool.block_size = 4096;
    config.limits.pool.max_leases = 64;
    Runtime::new(config).unwrap()
}

async fn deadline<F: Future>(future: F) -> F::Output {
    time::timeout(Duration::from_secs(10), future)
        .await
        .expect("native network capability operation timed out")
}

async fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let (client, server) = zip(TcpStream::connect(listener.local_addr()), listener.accept()).await;
    (client.unwrap(), server.unwrap())
}

fn payload(bytes: &[u8]) -> SendPayload {
    let pool = runtime::buffer_pool().unwrap();
    let mut buffer = pool.try_acquire_at_least(bytes.len()).unwrap();
    buffer.extend_from_slice(bytes).unwrap();
    SendPayload::Single(buffer.freeze())
}

async fn bytes_until_eof(stream: &TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(block) = stream.recv().await.unwrap() {
        bytes.extend_from_slice(block.as_slice());
    }
    bytes
}

#[test]
fn write_shutdown_is_lazy_and_preserves_reverse_response() {
    let mut runtime = runtime();
    runtime.block_on(deadline(async {
        let (client, server) = pair().await;
        let request = b"request after an unpolled shutdown";
        let response = b"response after the request's write half closes";

        drop(StreamShutdown::shutdown_write(&client));
        StreamSend::send_all(&client, payload(request))
            .await
            .result
            .unwrap();
        StreamSend::flush(&client).await.unwrap();
        StreamShutdown::shutdown_write(&client).await.unwrap();
        assert_eq!(bytes_until_eof(&server).await, request);

        server.send_all(payload(response)).await.result.unwrap();
        server.shutdown(Shutdown::Write).unwrap();
        assert_eq!(bytes_until_eof(&client).await, response);
    }));
}

#[test]
fn flush_validates_the_polling_worker_and_runtime_lifetime() {
    let mut owner = runtime();
    let (stream, _peer) = owner.block_on(deadline(pair()));

    // A future constructed in the owner must still reject a missing poll context.
    let flush = owner.block_on(std::future::poll_fn(|_| {
        std::task::Poll::Ready(StreamSend::flush(&stream))
    }));
    assert_eq!(
        block_on(poll_once(flush))
            .expect("native flush must complete on its first poll")
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );

    let mut other = runtime();
    let flush = owner.block_on(std::future::poll_fn(|_| {
        std::task::Poll::Ready(StreamSend::flush(&stream))
    }));
    assert_eq!(
        other
            .block_on(poll_once(flush))
            .expect("native flush must complete on its first poll")
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );

    // Conversely, construction outside a worker must not capture an error.
    let flush = StreamSend::flush(&stream);
    owner
        .block_on(poll_once(flush))
        .expect("native flush must complete on its first poll")
        .unwrap();

    let flush = owner.block_on(std::future::poll_fn(|_| {
        std::task::Poll::Ready(StreamSend::flush(&stream))
    }));
    drop(owner);
    assert_eq!(
        other
            .block_on(poll_once(flush))
            .expect("native flush must complete on its first poll")
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}
