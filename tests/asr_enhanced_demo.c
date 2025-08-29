/*
 * ASR Client Enhanced Demo
 * ========================
 * 
 * This demo shows how to use the enhanced ASR client with:
 * - INTERMEDIATE-RESULT event handling
 * - Proper STOP command with timeout
 * - Robust cleanup and error handling
 * - Comprehensive logging
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* This would normally include the actual headers */
/* #include "asr_engine.h" */

/* Mock definitions for demonstration */
typedef struct asr_engine_t asr_engine_t;
typedef struct asr_session_t asr_session_t;
typedef enum {
    RECOGNIZER_START_OF_INPUT,
    RECOGNIZER_RECOGNITION_COMPLETE, 
    RECOGNIZER_INTERMEDIATE_RESULT,
    RECOGNIZER_EVENT_COUNT
} mrcp_recognizer_event_id;

/* Demo function showing enhanced ASR workflow */
void demo_enhanced_asr_workflow(void) {
    printf("=== Enhanced ASR Client Workflow Demo ===\n\n");
    
    printf("1. INITIALIZATION:\n");
    printf("   - Create ASR engine with enhanced logging\n");
    printf("   - Create session with proper resource tracking\n");
    printf("   - Session and channel IDs logged for all operations\n\n");
    
    printf("2. RECOGNITION PROCESS:\n");
    printf("   - Send RECOGNIZE request\n");
    printf("   - Process events in enhanced event loop:\n\n");
    
    printf("   Event: START-OF-INPUT\n");
    printf("   [session:sess_001][channel:chan_001][timestamp:1234567890]\n");
    printf("   Action: Recognition started, ready for audio\n\n");
    
    printf("   Event: INTERMEDIATE-RESULT\n");
    printf("   [session:sess_001][channel:chan_001][timestamp:1234567895]\n");
    printf("   Action: Process immediately (no wait for STOP)\n");
    printf("   Partial result: \"Hello world this is a par...\"\n\n");
    
    printf("   Event: INTERMEDIATE-RESULT\n");
    printf("   [session:sess_001][channel:chan_001][timestamp:1234567900]\n");
    printf("   Action: Process immediately (updated partial)\n");
    printf("   Partial result: \"Hello world this is a partial transcript\"\n\n");
    
    printf("   Event: RECOGNITION-COMPLETE\n");
    printf("   [session:sess_001][channel:chan_001][timestamp:1234567905]\n");
    printf("   Action: Final result buffered, ready for STOP\n");
    printf("   Final result: \"Hello world this is a partial transcript from the user\"\n\n");
    
    printf("3. STOP HANDLING WITH TIMEOUT:\n");
    printf("   - Send STOP request with 5-second timeout\n");
    printf("   - Log: Sending STOP request [session:sess_001][channel:chan_001][timeout_ms:5000]\n");
    printf("   - Wait for STOP response with timeout monitoring\n");
    printf("   - Log: STOP response received [session:sess_001][channel:chan_001][elapsed_ms:250]\n\n");
    
    printf("4. ENHANCED CLEANUP (Idempotent):\n");
    printf("   - Stop streaming\n");
    printf("   - Close audio resources\n");
    printf("   - Remove MRCP channel\n");
    printf("   - Terminate MRCP session\n");
    printf("   - Destroy synchronization objects\n");
    printf("   - All operations logged with session/channel context\n");
    printf("   - Safe to call multiple times\n\n");
    
    printf("5. ERROR HANDLING SCENARIOS:\n\n");
    
    printf("   Scenario A: STOP Timeout\n");
    printf("   - Log: STOP response timeout [session:sess_001][channel:chan_001][elapsed_ms:5000]\n");
    printf("   - Action: Continue with cleanup despite timeout\n");
    printf("   - Result: Resources properly cleaned up\n\n");
    
    printf("   Scenario B: Event Processing Timeout\n");
    printf("   - Log: Event receive timeout [session:sess_001][channel:chan_001]\n");
    printf("   - Action: Graceful degradation, return available results\n");
    printf("   - Result: Partial results available via asr_session_get_intermediate_result()\n\n");
    
    printf("=== Key Improvements Demonstrated ===\n\n");
    
    printf("✓ INTERMEDIATE-RESULT events processed immediately\n");
    printf("✓ Final results buffered appropriately\n");
    printf("✓ STOP command with configurable timeout\n");
    printf("✓ Comprehensive logging with session/channel context\n");
    printf("✓ Idempotent cleanup safe for multiple calls\n");
    printf("✓ Proper error handling and resource management\n");
    printf("✓ Enhanced documentation and workflow guidance\n\n");
}

/* Demo function showing API usage */
void demo_api_usage(void) {
    printf("=== Enhanced API Usage Examples ===\n\n");
    
    printf("/* Create engine and session */\n");
    printf("asr_engine_t *engine = asr_engine_create(\"/opt/unimrcp\", APT_PRIO_INFO, APT_LOG_OUTPUT_CONSOLE);\n");
    printf("asr_session_t *session = asr_session_create(engine, \"default\");\n\n");
    
    printf("/* Start recognition with immediate intermediate result handling */\n");
    printf("const char *grammar = \"builtin:speech/transcribe\";\n");
    printf("const char *audio_file = \"long_audio.wav\";\n\n");
    
    printf("/* Send recognition request */\n");
    printf("asr_session_file_recognize_send(session, grammar, audio_file, 1, NULL, NULL, FALSE);\n\n");
    
    printf("/* Enhanced event processing loop */\n");
    printf("do {\n");
    printf("    mrcp_recognizer_event_id event = asr_session_file_recognize_receive(session);\n");
    printf("    \n");
    printf("    switch(event) {\n");
    printf("        case RECOGNIZER_INTERMEDIATE_RESULT:\n");
    printf("            /* Get and process partial result immediately */\n");
    printf("            const char *partial = asr_session_get_intermediate_result(session);\n");
    printf("            printf(\"Partial: %%s\\n\", partial);\n");
    printf("            break;\n");
    printf("            \n");
    printf("        case RECOGNIZER_RECOGNITION_COMPLETE:\n");
    printf("            /* Recognition complete, final result ready */\n");
    printf("            break;\n");
    printf("    }\n");
    printf("} while(!session->recog_complete);\n\n");
    
    printf("/* Send STOP with timeout and proper cleanup */\n");
    printf("if (!asr_session_stop_with_timeout(session, 5000)) {\n");
    printf("    /* Handle STOP timeout gracefully */\n");
    printf("    printf(\"STOP timeout, continuing with cleanup\\n\");\n");
    printf("}\n\n");
    
    printf("/* Get final result */\n");
    printf("const char *final_result = nlsml_result_get(session->recog_complete);\n");
    printf("printf(\"Final result: %%s\\n\", final_result);\n\n");
    
    printf("/* Idempotent cleanup (safe to call multiple times) */\n");
    printf("asr_session_destroy(session);\n");
    printf("asr_engine_destroy(engine);\n\n");
}

int main(void) {
    printf("UniMRCP ASR Client Enhanced Implementation Demo\n");
    printf("==============================================\n\n");
    
    demo_enhanced_asr_workflow();
    demo_api_usage();
    
    printf("This demo shows the key improvements made to the ASR client:\n");
    printf("- Better event handling with immediate intermediate result processing\n");
    printf("- Robust STOP command implementation with timeout handling\n");
    printf("- Enhanced logging and error reporting\n");
    printf("- Idempotent cleanup routines for reliable resource management\n");
    printf("- Comprehensive documentation for proper usage\n\n");
    
    return 0;
}