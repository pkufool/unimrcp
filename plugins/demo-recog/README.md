# Demo Recognition Engine with Real-time Streaming ASR

This is an enhanced version of the UniMRCP demo recognition engine that integrates with real-time streaming ASR services via WebSocket connections.

## Features

- **Real-time Audio Streaming**: Streams audio frames directly to ASR backend via WebSocket
- **Intermediate Results**: Supports partial recognition results through the new `INTERMEDIATE-RESULT` MRCP event
- **JSON Protocol**: Communicates with ASR backend using simple JSON message format
- **Thread Safety**: Uses ring buffers and mutex protection for asynchronous audio streaming
- **Configurable Backend**: Supports configurable ASR backend endpoints

## ASR Backend Configuration

The plugin connects to an ASR backend with the following default configuration:

- **Host**: `localhost`
- **Port**: `8080`
- **Path**: `/asr`
- **Protocol**: WebSocket with JSON messaging

### Configuration Details

The ASR backend endpoint can be configured by modifying the engine initialization in `demo_recog_engine.c`:

```c
/* Set default ASR backend configuration */
demo_engine->asr_backend_url = apr_pstrdup(pool, "your-asr-host");
demo_engine->asr_backend_port = apr_pstrdup(pool, "8080");
demo_engine->asr_backend_path = apr_pstrdup(pool, "/asr");
```

## ASR Backend Protocol

### WebSocket Connection

- The plugin establishes a WebSocket connection when a `RECOGNIZE` request is received
- Audio data is streamed as binary WebSocket messages
- The connection is closed when recognition completes or stops

### Message Format

The ASR backend should send JSON messages in the following format:

#### Partial Results (Intermediate)
```json
{
  "type": "partial",
  "result": "hello wor"
}
```

#### Final Results
```json
{
  "type": "final", 
  "result": "hello world"
}
```

### Audio Format

- Raw PCM audio data is streamed over the WebSocket connection
- Supports 8kHz and 16kHz sampling rates
- Linear PCM format (LPCM)

## MRCP Events

### New Event: INTERMEDIATE-RESULT

This implementation adds a new MRCP event `INTERMEDIATE-RESULT` for partial recognition hypotheses:

- **Event ID**: `RECOGNIZER_INTERMEDIATE_RESULT`
- **Purpose**: Sends partial recognition results as they become available
- **Content**: Plain text partial hypothesis
- **State**: `MRCP_REQUEST_STATE_INPROGRESS`

### Existing Events

- **START-OF-INPUT**: Sent when voice activity is detected
- **RECOGNITION-COMPLETE**: Sent when final recognition result is available

## Implementation Details

### Key Components

1. **WebSocket Integration**: Uses libwebsockets for real-time connection
2. **JSON Processing**: Uses json-c library for message parsing
3. **Audio Buffering**: Ring buffer queue for thread-safe audio streaming
4. **Event Generation**: MRCP event creation for partial and final results

### Thread Safety

- Audio queue protected by APR mutex
- WebSocket operations handled in separate context
- Asynchronous message passing for MRCP events

### Error Handling

- Connection failures result in `MRCP_STATUS_CODE_METHOD_FAILED`
- Invalid JSON responses are logged and ignored
- WebSocket disconnections are handled gracefully

## Building

The plugin requires the following additional dependencies:

- **libwebsockets-dev**: For WebSocket client functionality
- **libjson-c-dev**: For JSON message parsing

### Build Commands

```bash
# Install dependencies
sudo apt install libwebsockets-dev libjson-c-dev

# Build the plugin
cd plugins/demo-recog
make
```

## Testing

To test the streaming ASR integration:

1. **Start the Mock ASR Backend**:
   ```bash
   # Install Python dependencies
   pip install websockets asyncio
   
   # Start the test backend
   cd plugins/demo-recog
   python3 test_asr_backend.py --debug
   ```

2. **Configure UniMRCP Server**: 
   Ensure the demo recognition plugin is enabled in `conf/unimrcpserver.xml`:
   ```xml
   <plugin-factory>
     <engine id="Demo-Recog-1" name="demorecog" enable="true"/>
   </plugin-factory>
   ```

3. **Start UniMRCP Server**:
   ```bash
   cd platforms/unimrcp-server
   ./unimrcpserver -o 1
   ```

4. **Test with UniMRCP Client**:
   ```bash
   cd platforms/unimrcp-client
   ./unimrcpclient
   # Use option 2 (recognize) to test ASR streaming
   ```

5. **Monitor Events**: 
   You should see:
   - WebSocket connection established
   - Audio streaming to ASR backend
   - INTERMEDIATE-RESULT events with partial hypotheses
   - RECOGNITION-COMPLETE event with final result

### Expected Output

Mock ASR backend should show:
```
INFO:__main__:Starting mock ASR backend on localhost:8080/asr
INFO:__main__:New client connected from ('127.0.0.1', 54321)
DEBUG:__main__:Received 1024 bytes of audio data
INFO:__main__:Sent partial result: hello
INFO:__main__:Sent partial result: hello wor
INFO:__main__:Sent final result: hello world this is a test
```

UniMRCP server logs should show:
```
[RECOG-PLUGIN] Establishing WebSocket connection to ASR backend localhost:8080/asr
[RECOG-PLUGIN] WebSocket connection established
[RECOG-PLUGIN] Detected Voice Activity
[RECOG-PLUGIN] Processing ASR response: {"type":"partial","result":"hello"}
[RECOG-PLUGIN] Processing ASR response: {"type":"final","result":"hello world this is a test"}
```

## Example ASR Backend

A simple WebSocket ASR backend should:

1. Accept WebSocket connections on the configured endpoint
2. Receive binary audio data from the client
3. Process the audio in real-time
4. Send JSON messages for partial and final results
5. Close the connection when recognition is complete

Example using Node.js with `ws` library:

```javascript
const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: 8080, path: '/asr' });

wss.on('connection', function connection(ws) {
  ws.on('message', function incoming(data) {
    // Process audio data
    // Send partial results
    ws.send(JSON.stringify({
      type: 'partial',
      result: 'partial transcription...'
    }));
    
    // Send final result
    setTimeout(() => {
      ws.send(JSON.stringify({
        type: 'final',
        result: 'final transcription result'
      }));
    }, 1000);
  });
});
```

## Troubleshooting

### Common Issues

1. **Connection Failed**: Check ASR backend is running and accessible
2. **No Events**: Verify JSON message format from ASR backend
3. **Audio Issues**: Check sampling rate and codec compatibility
4. **Build Errors**: Ensure libwebsockets and json-c development packages are installed

### Logging

Enable debug logging in `logger.xml`:

```xml
<source name="RECOG-PLUGIN" priority="DEBUG" masking="NONE"/>
```

This will provide detailed logs of WebSocket connections, audio streaming, and message processing.