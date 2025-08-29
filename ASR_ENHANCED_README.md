# UniMRCP ASR Client Enhanced Implementation

This document describes the enhancements made to the UniMRCP ASR client to implement best practices for long audio transcription with proper STOP handling and session/channel closure.

## Overview

The enhanced ASR client implements the following key improvements:

1. **INTERMEDIATE-RESULT Event Support** - Process partial results immediately
2. **Enhanced STOP Handling** - Timeout-based STOP requests with proper error handling
3. **Robust Session Management** - Idempotent cleanup and proper resource management
4. **Comprehensive Logging** - Session/channel context with timestamps
5. **Enhanced Documentation** - Clear workflow guidance and event ordering

## Files Modified

### Core MRCP Resource Files
- `libs/mrcp/resources/include/mrcp_recog_resource.h` - Added INTERMEDIATE-RESULT event
- `libs/mrcp/resources/src/mrcp_recog_resource.c` - Added string mappings for new event

### ASR Client Implementation
- `platforms/libasr-client/include/asr_engine.h` - Enhanced session structure and new APIs
- `platforms/libasr-client/src/asr_engine.c` - Core implementation with all enhancements

### Test and Demo Files
- `tests/asr_enhanced_demo.c` - Comprehensive demo showing enhanced functionality
- `tests/asr_enhanced_test.c` - Validation tests for new features

## Key Enhancements

### 1. INTERMEDIATE-RESULT Event Support

**Problem**: Original implementation only handled START-OF-INPUT and RECOGNITION-COMPLETE events.

**Solution**: 
- Added `RECOGNIZER_INTERMEDIATE_RESULT` to the MRCP event enumeration
- Enhanced event processing to handle intermediate results immediately
- Added `asr_session_get_intermediate_result()` API for accessing partial results

```c
// New event added to enum
typedef enum {
    RECOGNIZER_START_OF_INPUT,
    RECOGNIZER_RECOGNITION_COMPLETE,
    RECOGNIZER_INTERPRETATION_COMPLETE,
    RECOGNIZER_INTERMEDIATE_RESULT,  // NEW
    RECOGNIZER_EVENT_COUNT
} mrcp_recognizer_event_id;
```

### 2. Enhanced Event Processing

**Problem**: Original event loops waited for completion without processing intermediate results.

**Solution**: Enhanced event processing in both file and stream recognition:

```c
switch(mrcp_message->start_line.method_id) {
    case RECOGNIZER_INTERMEDIATE_RESULT:
        /* Process INTERMEDIATE-RESULT immediately without waiting for STOP */
        apt_log(APT_LOG_MARK,APT_PRIO_INFO,"INTERMEDIATE-RESULT received [session:%s][channel:%s][timestamp:%" APR_TIME_T_FMT "]",
            session_id, channel_id, timestamp);
        asr_session->intermediate_result = mrcp_message;
        if (mrcp_message->body.buf) {
            apt_log(APT_LOG_MARK,APT_PRIO_DEBUG,"Partial result: %s", mrcp_message->body.buf);
        }
        break;
    // ... other cases
}
```

### 3. STOP Command with Timeout

**Problem**: No proper STOP handling with timeout management.

**Solution**: Implemented `asr_session_stop_with_timeout()` with:
- Configurable timeout (default 5 seconds)
- Proper error logging for timeouts
- Graceful degradation on timeout

```c
ASR_CLIENT_DECLARE(apt_bool_t) asr_session_stop_with_timeout(
    asr_session_t *asr_session,
    apr_time_t timeout_ms);
```

### 4. Enhanced Session Structure

**Problem**: Original session structure lacked state tracking for STOP handling.

**Solution**: Enhanced session structure with new fields:

```c
struct asr_session_t {
    // ... existing fields ...
    
    /** INTERMEDIATE-RESULT message buffer */
    mrcp_message_t           *intermediate_result;
    /** STOP request sent flag */
    apt_bool_t                stop_sent;
    /** STOP response received flag */
    apt_bool_t                stop_received;
    /** Session is being destroyed */
    apt_bool_t                destroying;
};
```

### 5. Idempotent Cleanup

**Problem**: Original cleanup was not idempotent and could fail on multiple calls.

**Solution**: Enhanced `asr_session_destroy_ex()` with:
- Idempotent resource cleanup (safe to call multiple times)
- Proper cleanup ordering
- Comprehensive logging of cleanup progress
- STOP request integration

### 6. Comprehensive Logging

**Problem**: Limited logging without session/channel context.

**Solution**: Enhanced logging throughout with:
- Session and channel IDs in all log messages
- Timestamps for event correlation
- Error context information
- Structured log format

Example log format:
```
[session:sess_001][channel:chan_001][timestamp:1234567890] INTERMEDIATE-RESULT received
[session:sess_001][channel:chan_001][timeout_ms:5000][elapsed_ms:250] STOP response received
```

## Event Processing Workflow

### Recommended Event Handling Order

1. **START-OF-INPUT** - Recognition started, ready for audio
2. **INTERMEDIATE-RESULT** (multiple) - Process immediately for live updates
3. **RECOGNITION-COMPLETE** - Final result ready
4. **STOP Request** - Send with timeout when recognition should end
5. **Cleanup** - Remove channel and terminate session

### Enhanced Event Loop Example

```c
do {
    mrcp_recognizer_event_id event = asr_session_file_recognize_receive(session);
    
    switch(event) {
        case RECOGNIZER_INTERMEDIATE_RESULT:
            /* Get and process partial result immediately */
            const char *partial = asr_session_get_intermediate_result(session);
            printf("Partial: %s\n", partial);
            break;
            
        case RECOGNIZER_RECOGNITION_COMPLETE:
            /* Recognition complete, final result ready */
            break;
            
        case RECOGNIZER_EVENT_COUNT:
            /* Timeout or error occurred */
            break;
    }
} while(!session->recog_complete);
```

## API Changes

### New Functions

```c
// Get intermediate result if available
ASR_CLIENT_DECLARE(const char*) asr_session_get_intermediate_result(asr_session_t *session);

// Send STOP request with timeout handling
ASR_CLIENT_DECLARE(apt_bool_t) asr_session_stop_with_timeout(
    asr_session_t *session,
    apr_time_t timeout_ms);
```

### Enhanced Functions

- `asr_session_file_recognize_receive()` - Now handles INTERMEDIATE-RESULT events
- `asr_session_stream_recognize()` - Enhanced event processing loop
- `asr_session_destroy_ex()` - Idempotent cleanup with STOP integration

## Error Handling

### Timeout Scenarios

1. **STOP Response Timeout** - Continue with cleanup, log error
2. **Event Processing Timeout** - Return available results, graceful degradation
3. **Session Cleanup Errors** - Continue with remaining cleanup steps

### Resource Management

- All cleanup functions are idempotent
- Resources cleaned in proper order: audio → channels → sessions
- Failed operations don't prevent subsequent cleanup
- Comprehensive error logging with context

## Testing

### Demo Program

Run the demo to see enhanced functionality:

```bash
cd tests
gcc -o asr_enhanced_demo asr_enhanced_demo.c
./asr_enhanced_demo
```

### Validation Tests

Run validation tests:

```bash
cd tests
gcc -o asr_enhanced_test asr_enhanced_test.c
./asr_enhanced_test
```

## Best Practices

### For Live Streaming Recognition

1. Process INTERMEDIATE-RESULT events immediately
2. Buffer final results until STOP response
3. Use appropriate timeout for STOP requests
4. Handle timeout gracefully with cleanup

### For File-based Recognition

1. Use enhanced event processing loop
2. Handle all event types appropriately
3. Implement proper error handling
4. Ensure resource cleanup on all paths

### For Long Audio Processing

1. Set appropriate timeouts based on audio length
2. Monitor and log intermediate results for progress
3. Handle network timeouts gracefully
4. Implement proper session lifecycle management

## Migration Guide

### From Original Implementation

1. **Event Handling**: Update event loops to handle INTERMEDIATE-RESULT
2. **Cleanup**: Replace direct cleanup calls with enhanced destroy functions
3. **Logging**: Update log statements to include session/channel context
4. **Error Handling**: Add proper timeout and error handling

### Code Changes Required

```c
// Old event loop
while(!asr_session->recog_complete) {
    mrcp_recognizer_event_id event = asr_session_file_recognize_receive(asr_session);
    // Basic handling only
}

// New enhanced event loop
do {
    mrcp_recognizer_event_id event = asr_session_file_recognize_receive(asr_session);
    switch(event) {
        case RECOGNIZER_INTERMEDIATE_RESULT:
            // Process immediately
            break;
        // Handle all event types
    }
} while(!asr_session->recog_complete);

// Enhanced cleanup with STOP
asr_session_stop_with_timeout(session, 5000);
asr_session_destroy(session);
```

## Performance Considerations

- INTERMEDIATE-RESULT processing adds minimal overhead
- Enhanced logging can be controlled via log levels
- Timeout handling adds robustness without performance impact
- Idempotent cleanup prevents resource leaks

## Future Enhancements

- Configurable intermediate result buffering
- Performance metrics collection
- Advanced timeout strategies
- Enhanced error recovery mechanisms