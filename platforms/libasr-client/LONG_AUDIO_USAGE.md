# Long Audio Transcription Usage Guide

This document explains how to use the enhanced UniMRCP ASR client for long audio transcription with automatic STOP message handling and partial results.

## Key Features

- **Automatic RECOGNIZER_STOP**: Client automatically sends STOP message when audio input is complete
- **Partial Results**: Handles RECOGNIZER_INTERPRETATION_COMPLETE events for real-time partial results
- **Graceful Cleanup**: Automatic resource cleanup and session management
- **Detailed Logging**: Comprehensive logging for monitoring transcription progress

## Basic Usage Example

```c
#include "asr_engine.h"

int main() {
    asr_engine_t *engine;
    asr_session_t *session;
    const char *result;
    
    // 1. Create ASR engine
    engine = asr_engine_create("/path/to/unimrcp", APT_PRIO_INFO, APT_LOG_OUTPUT_CONSOLE);
    if (!engine) {
        printf("Failed to create ASR engine\n");
        return -1;
    }
    
    // 2. Create ASR session
    session = asr_session_create(engine, "uni2");
    if (!session) {
        printf("Failed to create ASR session\n");
        asr_engine_destroy(engine);
        return -1;
    }
    
    // 3. Perform long audio transcription
    // This will:
    // - Stream the audio file to the server
    // - Automatically send RECOGNIZER_STOP when file is complete
    // - Handle partial results during recognition
    // - Return final transcription result
    result = asr_session_file_recognize(session, 
                                       "grammar.xml",     // Grammar file
                                       "long_audio.pcm",  // Audio file (can be long)
                                       NULL,              // Params file (optional)
                                       FALSE);            // Send set params
    
    if (result) {
        printf("Final transcription result: %s\n", result);
    } else {
        printf("Transcription failed\n");
    }
    
    // 4. Cleanup
    asr_session_destroy(session);
    asr_engine_destroy(engine);
    
    return 0;
}
```

## Advanced Usage with Event Handling

For more control over the transcription process, you can use the split functions:

```c
// Send recognition request
asr_session_file_recognize_send(session, "grammar.xml", "long_audio.pcm", 1, weights, NULL, FALSE);

// Handle events in a loop
mrcp_recognizer_event_id event_id;
do {
    // Check and send STOP message if audio input is complete
    asr_session_check_and_stop(session);
    
    // Receive next event
    event_id = asr_session_file_recognize_receive(session);
    
    switch (event_id) {
        case RECOGNIZER_START_OF_INPUT:
            printf("Recognition started\n");
            break;
            
        case RECOGNIZER_INTERPRETATION_COMPLETE:
            printf("Partial result received\n");
            // Partial results are automatically logged
            break;
            
        case RECOGNIZER_RECOGNITION_COMPLETE:
            printf("Recognition completed\n");
            break;
            
        case RECOGNIZER_EVENT_COUNT:
            printf("Timeout or error\n");
            break;
    }
} while (event_id != RECOGNIZER_RECOGNITION_COMPLETE && event_id != RECOGNIZER_EVENT_COUNT);

// Get final result
const char *result = nlsml_result_get(session->recog_complete);
```

## Workflow Details

1. **Audio Streaming**: The client streams audio frames to the MRCP server
2. **Input Complete Detection**: When the audio file ends, `asr_stream_read()` sets the `input_complete` flag
3. **Automatic STOP**: The receive function detects the flag and sends `RECOGNIZER_STOP` message
4. **STOP Response**: Client waits for and handles the STOP response, logging success/failure
5. **Partial Results**: `RECOGNIZER_INTERPRETATION_COMPLETE` events are handled and logged during recognition
6. **Final Results**: `RECOGNIZER_RECOGNITION_COMPLETE` event provides final transcription
7. **Cleanup**: Audio file is closed and session resources are cleaned up

## Logging Output

The enhanced client provides detailed logging:

```
[INFO] Audio input complete, STOP message will be sent
[INFO] Sending RECOGNIZER_STOP Request
[INFO] RECOGNIZER_STOP Request Successful
[INFO] INTERPRETATION-COMPLETE (partial result) received
[INFO] Partial result parsed successfully
[INFO] RECOGNTION-COMPLETE received
[INFO] Final result parsed successfully
[INFO] Starting graceful session closure
[INFO] Audio file closed during graceful closure
[INFO] Graceful session closure completed
```

## Error Handling

The implementation includes comprehensive error handling:

- Failed STOP message creation is logged and handled gracefully
- STOP request failures are logged but don't prevent final result retrieval
- Timeout conditions return `RECOGNIZER_EVENT_COUNT`
- Resource cleanup is performed even in error scenarios

## Configuration Requirements

Ensure your MRCP server configuration supports:
- Long audio sessions (appropriate timeouts)
- Partial result generation (if desired)
- Proper STOP message handling

This enhanced implementation ensures reliable long audio transcription with proper MRCP protocol compliance and resource management.