use crate::{
    buffers::Acker,
    event::{self, Event},
    sinks::util::SinkExt,
    template::Template,
    topology::config::DataType,
};

use futures::{future, Async, AsyncSink, Sink, StartSend};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    Text,
    Json,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FileSinkConfig {
    pub path: String,
    pub close_timeout_secs: Option<u64>,
    pub encoding: Option<Encoding>,
}

impl FileSinkConfig {
    pub fn new(path: String) -> Self {
        Self {
            path,
            close_timeout_secs: None,
            encoding: None,
        }
    }
}

#[typetag::serde(name = "file")]
impl crate::topology::config::SinkConfig for FileSinkConfig {
    fn build(&self, acker: Acker) -> Result<(super::RouterSink, super::Healthcheck), String> {
        let sink = PartitionedFileSink::new(
            Template::from(&self.path[..]),
            self.close_timeout_secs,
            self.encoding.clone(),
        )
        .stream_ack(acker);

        Ok((Box::new(sink), Box::new(future::ok(()))))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }
}

pub type EmbeddedFileSink = Box<dyn Sink<SinkItem = Event, SinkError = ()> + 'static + Send>;

pub struct PartitionedFileSink {
    path: Template,
    encoding: Option<Encoding>,
    close_timeout_secs: Option<u64>,

    partitions: HashMap<Arc<Path>, EmbeddedFileSink>,
    last_accessed: HashMap<Arc<Path>, Instant>,
    closing: Vec<EmbeddedFileSink>,
}

impl PartitionedFileSink {
    pub fn new(
        path: Template,
        close_timeout_secs: Option<u64>,
        encoding: Option<Encoding>,
    ) -> Self {
        PartitionedFileSink {
            path,
            encoding,
            close_timeout_secs,
            partitions: HashMap::new(),
            last_accessed: HashMap::new(),
            closing: Vec::new(),
        }
    }

    fn collect_old_files(&mut self) {
        let mut recently_outdated = Vec::new();
        for timeout in self.close_timeout_secs {
        if let Some(timeout) = self.close_timeout_secs {
            self.last_accessed.retain(|path, time| {
                if time.elapsed().as_secs() > timeout {
                    debug!(message = "removing file.", file = ?path);
                    recently_outdated.push(path.clone());
                    false
                } else {
                    true
                }
            });
        }

        let mut recently_outdated = recently_outdated
            .into_iter()
            .map(|ref path| self.partitions.remove(path).unwrap())
            .collect();

        //it is easier to empty a `closing` buffer and then put back all sinks which are not closed yet (see `poll_close`)
        let mut closing: Vec<EmbeddedFileSink> = self.closing.drain(..).collect();
        closing.append(&mut recently_outdated);

        for file in closing.into_iter() {
            self.poll_close(file);
        }
    }

    fn poll_close(&mut self, mut file: EmbeddedFileSink) {
        match file.close() {
            Err(err) => error!("Error while closing FileSink: {:?}", err),
            Ok(Async::Ready(())) => debug!("a FileSink closed"),
            Ok(Async::NotReady) => self.closing.push(file),
        }
    }

    fn opened_files(&self) -> Vec<Arc<Path>> {
        self.partitions.keys().map(|path| path.clone()).collect()
    }
}

impl Sink for PartitionedFileSink {
    type SinkItem = Event;
    type SinkError = ();

    fn start_send(&mut self, event: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self.path.render(&event) {
            Ok(bytes) => match std::str::from_utf8(&bytes) {
                Err(err) => {
                    warn!(
                        message = "Path produced is not valid UTF-8. Dropping event.",
                        ?err
                    );
                    Ok(AsyncSink::Ready)
                }

                Ok(path) => {
                    let path: Arc<Path> = Arc::from(PathBuf::from(path));

                    if self.close_timeout_secs.is_some() {
                        self.last_accessed.insert(path.clone(), Instant::now());
                    }

                    let ref mut partition = match self.partitions.entry(path) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            let path = entry.key();
                            let encoding = self.encoding.clone();
                            let sink = internal::FileSink::new_with_encoding(&path, encoding);
                            entry.insert(sink)
                        }
                    };

                    partition.start_send(event)
                }
            },

            Err(missing_keys) => {
                warn!(
                    message = "Keys do not exist on the event. Dropping event.",
                    ?missing_keys
                );
                Ok(AsyncSink::Ready)
            }
        }
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        self.partitions
            .iter_mut()
            .for_each(|(path, partition)| match partition.poll_complete() {
                Ok(_) => {}
                Err(()) => error!("Error in downstream FileSink with path {:?}", path),
            });

        self.collect_old_files();

        debug!(message = "keeping opened", files = ?self.opened_files());

        Ok(Async::Ready(()))
    }
}

mod internal {

    use super::*;
    use bytes::Bytes;
    use std::io::{self, ErrorKind};
    use std::path::{Path, PathBuf};

    use futures::{try_ready, Async, AsyncSink, Future, Poll, Sink, StartSend};
    use tokio::codec::{BytesCodec, FramedWrite};
    use tokio::fs::file::{File, OpenFuture};
    use tokio::fs::OpenOptions;

    use tracing::field;

    pub struct FileSink {
        pub path: PathBuf,
        state: FileSinkState,
    }

    enum FileSinkState {
        Disconnected,
        OpeningFile(OpenFuture<PathBuf>),
        FileProvided(FramedWrite<File, BytesCodec>),
    }

    impl FileSinkState {
        fn init(path: PathBuf) -> Self {
            debug!(message = "opening", file = ?path);
            let mut options = OpenOptions::new();
            options.create(true).append(true);

            FileSinkState::OpeningFile(options.open(path))
        }
    }

    impl FileSink {
        pub fn new(path: PathBuf) -> Self {
            Self {
                path: path.clone(),
                state: FileSinkState::init(path),
            }
        }

        pub fn new_with_encoding(path: &Path, encoding: Option<Encoding>) -> EmbeddedFileSink {
            let sink = FileSink::new(path.to_path_buf())
                .sink_map_err(|err| error!("Terminating the sink due to error: {}", err))
                .with(move |event| Self::encode_event(event, &encoding));

            Box::new(sink)
        }

        pub fn poll_file(&mut self) -> Poll<&mut FramedWrite<File, BytesCodec>, io::Error> {
            loop {
                match self.state {
                    FileSinkState::Disconnected => return Err(disconnected()),

                    FileSinkState::FileProvided(ref mut sink) => return Ok(Async::Ready(sink)),

                    FileSinkState::OpeningFile(ref mut open_future) => match open_future.poll() {
                        Ok(Async::Ready(file)) => {
                            debug!(message = "provided", file = ?file);
                            self.state = FileSinkState::FileProvided(FramedWrite::new(
                                file,
                                BytesCodec::new(),
                            ));
                        }
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(err) => {
                            self.state = FileSinkState::Disconnected;
                            return Err(err);
                        }
                    },
                }
            }
        }

        fn encode_event(event: Event, encoding: &Option<Encoding>) -> Result<Bytes, ()> {
            let log = event.into_log();

            let result = match (encoding, log.is_structured()) {
                (&Some(Encoding::Json), _) | (_, true) => serde_json::to_vec(&log.all_fields())
                    .map_err(|e| panic!("Error encoding: {}", e)),

                (&Some(Encoding::Text), _) | (_, false) => Ok(log
                    .get(&event::MESSAGE)
                    .map(|v| v.as_bytes().to_vec())
                    .unwrap_or(Vec::new())),
            };

            result.map(|mut bytes| {
                bytes.push(b'\n');
                Bytes::from(bytes)
            })
        }
    }

    impl Sink for FileSink {
        type SinkItem = Bytes;
        type SinkError = io::Error;

        fn start_send(
            &mut self,
            line: Self::SinkItem,
        ) -> StartSend<Self::SinkItem, Self::SinkError> {
            match self.poll_file() {
                Ok(Async::Ready(file)) => {
                    debug!(
                        message = "sending event",
                        bytes = &field::display(line.len())
                    );
                    match file.start_send(line) {
                        Ok(ok) => Ok(ok),

                        Err(err) => {
                            self.state = FileSinkState::Disconnected;
                            Err(err)
                        }
                    }
                }
                Ok(Async::NotReady) => Ok(AsyncSink::NotReady(line)),
                Err(err) => Err(err),
            }
        }

        fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
            if let FileSinkState::Disconnected = self.state {
                return Err(disconnected());
            }

            let file = try_ready!(self.poll_file());

            match file.poll_complete() {
                Err(err) => {
                    error!("Error while completing {:?}: {}", self.path, err);
                    self.state = FileSinkState::Disconnected;
                    Ok(Async::Ready(()))
                }
                Ok(ok) => Ok(ok),
            }
        }

        fn close(&mut self) -> Poll<(), Self::SinkError> {
            match self.poll_complete() {
                Ok(Async::Ready(())) => match self.state {
                    FileSinkState::Disconnected => Ok(Async::Ready(())),

                    FileSinkState::FileProvided(ref mut sink) => sink.close(),

                    //this state is eliminated during poll_complete()
                    FileSinkState::OpeningFile(_) => unreachable!(),
                },
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(err) => Err(err),
            }
        }
    }

    fn disconnected() -> io::Error {
        io::Error::new(ErrorKind::NotConnected, "FileSink is in disconnected state")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        buffers::Acker,
        event::{self, Event},
        test_util::{lines_from_file, random_events_with_stream, random_lines_with_stream},
        topology::config::SinkConfig,
    };

    use core::convert::From;
    use futures::{stream, Stream};
    use tempfile::tempdir;

    #[test]
    fn without_partitions() {
        let directory = tempdir().unwrap();

        let mut template = directory.into_path().to_string_lossy().to_string();
        template.push_str("/test.out");

        let config = FileSinkConfig::new(template.clone());

        let (sink, _) = config.build(Acker::Null).unwrap();
        let (input, events) = random_lines_with_stream(100, 64);

        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let pump = sink.send_all(events);
        let _ = rt.block_on(pump).unwrap();

        let output = lines_from_file(template);
        for (input, output) in input.into_iter().zip(output) {
            assert_eq!(input, output);
        }
    }

    #[test]
    fn partitions_are_created_dynamically() {
        let directory = tempdir().unwrap();
        let directory = directory.into_path();

        let mut template = directory.to_string_lossy().to_string();
        template.push_str("/{{level}}s-{{date}}.log");

        let config = FileSinkConfig::new(template.clone());

        let (sink, _) = config.build(Acker::Null).unwrap();

        let (mut input, _) = random_events_with_stream(32, 8);
        input[0]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-26-07".into());
        input[0]
            .as_mut_log()
            .insert_implicit("level".into(), "warning".into());
        input[1]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-26-07".into());
        input[1]
            .as_mut_log()
            .insert_implicit("level".into(), "error".into());
        input[2]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-26-07".into());
        input[2]
            .as_mut_log()
            .insert_implicit("level".into(), "warning".into());
        input[3]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-27-07".into());
        input[3]
            .as_mut_log()
            .insert_implicit("level".into(), "error".into());
        input[4]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-27-07".into());
        input[4]
            .as_mut_log()
            .insert_implicit("level".into(), "warning".into());
        input[5]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-27-07".into());
        input[5]
            .as_mut_log()
            .insert_implicit("level".into(), "warning".into());
        input[6]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-28-07".into());
        input[6]
            .as_mut_log()
            .insert_implicit("level".into(), "warning".into());
        input[7]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-29-07".into());
        input[7]
            .as_mut_log()
            .insert_implicit("level".into(), "error".into());

        let events = stream::iter_ok(input.clone().into_iter());
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let pump = sink.send_all(events);
        let _ = rt.block_on(pump).unwrap();

        let output = vec![
            lines_from_file(&directory.join("warnings-2019-26-07.log")),
            lines_from_file(&directory.join("errors-2019-26-07.log")),
            lines_from_file(&directory.join("warnings-2019-27-07.log")),
            lines_from_file(&directory.join("errors-2019-27-07.log")),
            lines_from_file(&directory.join("warnings-2019-28-07.log")),
            lines_from_file(&directory.join("errors-2019-29-07.log")),
        ];

        assert_eq!(
            input[0].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[0][0])
        );
        assert_eq!(
            input[1].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[1][0])
        );
        assert_eq!(
            input[2].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[0][1])
        );
        assert_eq!(
            input[3].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[3][0])
        );
        assert_eq!(
            input[4].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[2][0])
        );
        assert_eq!(
            input[5].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[2][1])
        );
        assert_eq!(
            input[6].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[4][0])
        );
        assert_eq!(
            input[7].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[5][0])
        );
    }

    #[test]
    fn text_output_is_correct() {
        let (input, events) = random_lines_with_stream(100, 16);
        let output = test_unpartitioned_with_encoding(events, Encoding::Text, None);

        for (input, output) in input.into_iter().zip(output) {
            assert_eq!(input, output);
        }
    }

    #[test]
    fn json_output_is_correct() {
        let (input, events) = random_lines_with_stream(100, 16);
        let output = test_unpartitioned_with_encoding(events, Encoding::Json, None);

        for (input, output) in input.into_iter().zip(output) {
            let output: serde_json::Value = serde_json::from_str(&output[..]).unwrap();
            let output = output.get("message").and_then(|v| v.as_str()).unwrap();
            assert_eq!(input, output);
        }
    }

    #[test]
    fn file_is_appended_not_truncated() {
        let directory = tempdir().unwrap().into_path();

        let (mut input1, events) = random_lines_with_stream(100, 16);
        test_unpartitioned_with_encoding(events, Encoding::Text, Some(directory.clone()));

        let (mut input2, events) = random_lines_with_stream(100, 16);
        let output = test_unpartitioned_with_encoding(events, Encoding::Text, Some(directory));

        let mut input = vec![];
        input.append(&mut input1);
        input.append(&mut input2);

        assert_eq!(output.len(), input.len());

        for (input, output) in input.into_iter().zip(output) {
            assert_eq!(input, output);
        }
    }

    fn test_unpartitioned_with_encoding<S>(
        events: S,
        encoding: Encoding,
        directory: Option<PathBuf>,
    ) -> Vec<String>
    where
        S: 'static + Stream<Item = Event, Error = ()> + Send,
    {
        let path = directory
            .unwrap_or(tempdir().unwrap().into_path())
            .join("test.out");

        let sink = internal::FileSink::new_with_encoding(&path, Some(encoding));

        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let pump = sink.send_all(events);
        let _ = rt.block_on(pump).unwrap();

        lines_from_file(&path)
    }
}
