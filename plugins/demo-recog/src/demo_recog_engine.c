/*
 * Copyright 2008-2015 Arsen Chaloyan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* 
 * Mandatory rules concerning plugin implementation.
 * 1. Each plugin MUST implement a plugin/engine creator function
 *    with the exact signature and name (the main entry point)
 *        MRCP_PLUGIN_DECLARE(mrcp_engine_t*) mrcp_plugin_create(apr_pool_t *pool)
 * 2. Each plugin MUST declare its version number
 *        MRCP_PLUGIN_VERSION_DECLARE
 * 3. One and only one response MUST be sent back to the received request.
 * 4. Methods (callbacks) of the MRCP engine channel MUST not block.
 *   (asynchronous response can be sent from the context of other thread)
 * 5. Methods (callbacks) of the MPF engine stream MUST not block.
 * 
 * STREAMING ASR ENHANCEMENTS:
 * This demo recognition engine has been enhanced with real-time streaming ASR
 * capabilities using libwebsockets:
 * - Establishes WebSocket connections to configurable ASR backends
 * - Streams audio frames in real-time during recognition
 * - Supports INTERMEDIATE-RESULT events for partial recognition hypotheses
 * - Processes JSON responses from ASR backend for partial and final results
 * - Thread-safe audio buffering using APR ring queues
 * - Default ASR backend: localhost:8080/asr
 */

#include "mrcp_recog_engine.h"
#include "mpf_activity_detector.h"
#include "apt_consumer_task.h"
#include "apt_log.h"
#include "mrcp_engine_impl.h"
#include <libwebsockets.h>
#include <json-c/json.h>
#include <string.h>
#include <pthread.h>
#include <stdlib.h>

#define RECOG_ENGINE_TASK_NAME "Demo Recog Engine"

typedef struct demo_recog_engine_t demo_recog_engine_t;
typedef struct demo_recog_channel_t demo_recog_channel_t;
typedef struct demo_recog_msg_t demo_recog_msg_t;
typedef struct demo_recog_audio_buffer_t demo_recog_audio_buffer_t;

/** Audio buffer for WebSocket streaming */
struct demo_recog_audio_buffer_t {
  /** Ring element */
  APR_RING_ENTRY(demo_recog_audio_buffer_t) link;
  /** Audio data */
  void *data;
  /** Data size */
  apr_size_t size;
};

/** Declaration of recognizer engine methods */
static apt_bool_t demo_recog_engine_destroy(mrcp_engine_t *engine);
static apt_bool_t demo_recog_engine_open(mrcp_engine_t *engine);
static apt_bool_t demo_recog_engine_close(mrcp_engine_t *engine);
static mrcp_engine_channel_t* demo_recog_engine_channel_create(mrcp_engine_t *engine, apr_pool_t *pool);

static const struct mrcp_engine_method_vtable_t engine_vtable = {
  demo_recog_engine_destroy,
  demo_recog_engine_open,
  demo_recog_engine_close,
  demo_recog_engine_channel_create
};


/** Declaration of recognizer channel methods */
static apt_bool_t demo_recog_channel_destroy(mrcp_engine_channel_t *channel);
static apt_bool_t demo_recog_channel_open(mrcp_engine_channel_t *channel);
static apt_bool_t demo_recog_channel_close(mrcp_engine_channel_t *channel);
static apt_bool_t demo_recog_channel_request_process(mrcp_engine_channel_t *channel, mrcp_message_t *request);

static const struct mrcp_engine_channel_method_vtable_t channel_vtable = {
  demo_recog_channel_destroy,
  demo_recog_channel_open,
  demo_recog_channel_close,
  demo_recog_channel_request_process
};

/** Declaration of recognizer audio stream methods */
static apt_bool_t demo_recog_stream_destroy(mpf_audio_stream_t *stream);
static apt_bool_t demo_recog_stream_open(mpf_audio_stream_t *stream, mpf_codec_t *codec);
static apt_bool_t demo_recog_stream_close(mpf_audio_stream_t *stream);
static apt_bool_t demo_recog_stream_write(mpf_audio_stream_t *stream, const mpf_frame_t *frame);

static const mpf_audio_stream_vtable_t audio_stream_vtable = {
  demo_recog_stream_destroy,
  NULL,
  NULL,
  NULL,
  demo_recog_stream_open,
  demo_recog_stream_close,
  demo_recog_stream_write,
  NULL
};

/** Declaration of demo recognizer engine */
struct demo_recog_engine_t {
  apt_consumer_task_t    *task;
  /** ASR backend configuration */
  char                   *asr_backend_url;
  char                   *asr_backend_port;
  char                   *asr_backend_path;
};

/** Declaration of demo recognizer channel */
struct demo_recog_channel_t {
  /** Back pointer to engine */
  demo_recog_engine_t     *demo_engine;
  /** Engine channel base */
  mrcp_engine_channel_t   *channel;

  /** Active (in-progress) recognition request */
  mrcp_message_t          *recog_request;
  /** Pending stop response */
  mrcp_message_t          *stop_response;
  /** Indicates whether input timers are started */
  apt_bool_t               timers_started;
  /** Voice activity detector */
  mpf_activity_detector_t *detector;
  /** File to write utterance to */
  FILE                    *audio_out;

  pthread_t thread;

  /** WebSocket connection context */
  struct lws_context      *ws_context;
  /** WebSocket connection instance */
  struct lws              *ws_connection;
  /** WebSocket connection state */
  apt_bool_t               ws_connected;
  /** ASR backend configuration */
  char                    *asr_backend_url;
  char                    *asr_backend_port;
  char                    *asr_backend_path;
  apt_bool_t              input_finished;
  /** Audio buffering for WebSocket streaming */
  APR_RING_HEAD(demo_recog_audio_buffer_head_t, demo_recog_audio_buffer_t) audio_queue;
  apr_thread_mutex_t      *audio_queue_mutex;
};

typedef enum {
  DEMO_RECOG_MSG_OPEN_CHANNEL,
  DEMO_RECOG_MSG_CLOSE_CHANNEL,
  DEMO_RECOG_MSG_REQUEST_PROCESS
} demo_recog_msg_type_e;

/** Declaration of demo recognizer task message */
struct demo_recog_msg_t {
  demo_recog_msg_type_e  type;
  mrcp_engine_channel_t *channel; 
  mrcp_message_t        *request;
};

static apt_bool_t demo_recog_msg_signal(demo_recog_msg_type_e type, mrcp_engine_channel_t *channel, mrcp_message_t *request);
static apt_bool_t demo_recog_msg_process(apt_task_t *task, apt_task_msg_t *msg);

/** WebSocket callback function */
static int demo_recog_ws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len);

/** WebSocket helper functions */
static apt_bool_t demo_recog_ws_connect(demo_recog_channel_t *recog_channel);
static apt_bool_t demo_recog_ws_disconnect(demo_recog_channel_t *recog_channel);
static apt_bool_t demo_recog_ws_send_audio(demo_recog_channel_t *recog_channel, const void *data, apr_size_t size);
static apt_bool_t demo_recog_process_asr_response(demo_recog_channel_t *recog_channel, const char *json_msg);

/** INTERMEDIATE-RESULT event handler */
static apt_bool_t demo_recog_intermediate_result(demo_recog_channel_t *recog_channel, const char *result_text);

/** Declare this macro to set plugin version */
MRCP_PLUGIN_VERSION_DECLARE

/**
 * Declare this macro to use log routine of the server, plugin is loaded from.
 * Enable/add the corresponding entry in logger.xml to set a cutsom log source priority.
 *    <source name="RECOG-PLUGIN" priority="DEBUG" masking="NONE"/>
 */
MRCP_PLUGIN_LOG_SOURCE_IMPLEMENT(RECOG_PLUGIN,"RECOG-PLUGIN")

/** Use custom log source mark */
#define RECOG_LOG_MARK   APT_LOG_MARK_DECLARE(RECOG_PLUGIN)

/** Create demo recognizer engine */
MRCP_PLUGIN_DECLARE(mrcp_engine_t*) mrcp_plugin_create(apr_pool_t *pool)
{
  demo_recog_engine_t *demo_engine = apr_palloc(pool,sizeof(demo_recog_engine_t));
  apt_task_t *task;
  apt_task_vtable_t *vtable;
  apt_task_msg_pool_t *msg_pool;

  /* Set default ASR backend configuration */
  const char *asr_url = getenv("ASR_SERVER_IP");
  if(asr_url) {
    demo_engine->asr_backend_url = apr_pstrdup(pool, asr_url);
  } else {
    demo_engine->asr_backend_url = apr_pstrdup(pool, "127.0.0.1");
  }
  const char *asr_port = getenv("ASR_SERVER_PORT");
  if(asr_port) {
    demo_engine->asr_backend_port = apr_pstrdup(pool, asr_port);
  } else {
    demo_engine->asr_backend_port = apr_pstrdup(pool, "6006");
  }
  const char *asr_path = getenv("ASR_SERVER_PATH");
  if(asr_path) {
    demo_engine->asr_backend_path = apr_pstrdup(pool, asr_path);
  } else {
    demo_engine->asr_backend_path = apr_pstrdup(pool, "/");
  }

  msg_pool = apt_task_msg_pool_create_dynamic(sizeof(demo_recog_msg_t),pool);
  demo_engine->task = apt_consumer_task_create(demo_engine,msg_pool,pool);
  if(!demo_engine->task) {
    return NULL;
  }
  task = apt_consumer_task_base_get(demo_engine->task);
  apt_task_name_set(task,RECOG_ENGINE_TASK_NAME);
  vtable = apt_task_vtable_get(task);
  if(vtable) {
    vtable->process_msg = demo_recog_msg_process;
  }

  /* create engine base */
  return mrcp_engine_create(
        MRCP_RECOGNIZER_RESOURCE,  /* MRCP resource identifier */
        demo_engine,               /* object to associate */
        &engine_vtable,            /* virtual methods table of engine */
        pool);                     /* pool to allocate memory from */
}

/** Destroy recognizer engine */
static apt_bool_t demo_recog_engine_destroy(mrcp_engine_t *engine)
{
  demo_recog_engine_t *demo_engine = engine->obj;
  if(demo_engine->task) {
    apt_task_t *task = apt_consumer_task_base_get(demo_engine->task);
    apt_task_destroy(task);
    demo_engine->task = NULL;
  }
  return TRUE;
}

/** Open recognizer engine */
static apt_bool_t demo_recog_engine_open(mrcp_engine_t *engine)
{
  demo_recog_engine_t *demo_engine = engine->obj;
  if(demo_engine->task) {
    apt_task_t *task = apt_consumer_task_base_get(demo_engine->task);
    apt_task_start(task);
  }
  return mrcp_engine_open_respond(engine,TRUE);
}

/** Close recognizer engine */
static apt_bool_t demo_recog_engine_close(mrcp_engine_t *engine)
{
  demo_recog_engine_t *demo_engine = engine->obj;
  if(demo_engine->task) {
    apt_task_t *task = apt_consumer_task_base_get(demo_engine->task);
    apt_task_terminate(task,TRUE);
  }
  return mrcp_engine_close_respond(engine);
}

static mrcp_engine_channel_t* demo_recog_engine_channel_create(mrcp_engine_t *engine, apr_pool_t *pool)
{
  mpf_stream_capabilities_t *capabilities;
  mpf_termination_t *termination; 

  /* create demo recog channel */
  demo_recog_channel_t *recog_channel = apr_palloc(pool,sizeof(demo_recog_channel_t));
  recog_channel->demo_engine = engine->obj;
  recog_channel->recog_request = NULL;
  recog_channel->stop_response = NULL;
  recog_channel->detector = mpf_activity_detector_create(pool);
  recog_channel->audio_out = NULL;

  /* Initialize WebSocket fields */
  recog_channel->ws_context = NULL;
  recog_channel->ws_connection = NULL;
  recog_channel->ws_connected = FALSE;
  recog_channel->input_finished = FALSE;
  recog_channel->asr_backend_url = apr_pstrdup(pool, recog_channel->demo_engine->asr_backend_url);
  recog_channel->asr_backend_port = apr_pstrdup(pool, recog_channel->demo_engine->asr_backend_port);
  recog_channel->asr_backend_path = apr_pstrdup(pool, recog_channel->demo_engine->asr_backend_path);
  
  /* Initialize audio queue */
  APR_RING_INIT(&recog_channel->audio_queue, demo_recog_audio_buffer_t, link); 
  apr_thread_mutex_create(&recog_channel->audio_queue_mutex, APR_THREAD_MUTEX_UNNESTED, pool);

  capabilities = mpf_sink_stream_capabilities_create(pool);
  mpf_codec_capabilities_add(
      &capabilities->codecs,
      MPF_SAMPLE_RATE_8000 | MPF_SAMPLE_RATE_16000,
      "LPCM");

  /* create media termination */
  termination = mrcp_engine_audio_termination_create(
      recog_channel,        /* object to associate */
      &audio_stream_vtable, /* virtual methods table of audio stream */
      capabilities,         /* stream capabilities */
      pool);                /* pool to allocate memory from */

  /* create engine channel base */
  recog_channel->channel = mrcp_engine_channel_create(
      engine,               /* engine */
      &channel_vtable,      /* virtual methods table of engine channel */
      recog_channel,        /* object to associate */
      termination,          /* associated media termination */
      pool);                /* pool to allocate memory from */

  return recog_channel->channel;
}

/** Destroy engine channel */
static apt_bool_t demo_recog_channel_destroy(mrcp_engine_channel_t *channel)
{
  demo_recog_channel_t *recog_channel = channel->method_obj;

  /* Cleanup WebSocket connection */
  if(recog_channel->ws_connected) {
    demo_recog_ws_disconnect(recog_channel);
  }

  /* Cleanup audio queue */
  if(recog_channel->audio_queue_mutex) {
    apr_thread_mutex_lock(recog_channel->audio_queue_mutex);
    while(!APR_RING_EMPTY(&recog_channel->audio_queue, demo_recog_audio_buffer_t, link)) {
      demo_recog_audio_buffer_t *buffer = APR_RING_FIRST(&recog_channel->audio_queue);
      APR_RING_REMOVE(buffer, link);
    }
    apr_thread_mutex_unlock(recog_channel->audio_queue_mutex);
  }

  return TRUE;
}

/*
static void process_mrcp_channel(mrcp_engine_channel_t *channel) {
    const mpf_stream_capabilities_t *capabilities = mrcp_application_audio_stream_capabilities_get(channel);
    const mpf_stream_capability_t *audio_cap = mpf_stream_capabilities_find(capabilities, MPF_DIRECTION_RECV);
    if(audio_cap) {
        const mpf_codec_descriptor_t *codec = mpf_stream_capability_codec_descriptor_get(audio_cap, 0);
        if(codec) {
            apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Recevied audio format:\n");
            apt_log(RECOG_LOG_MARK,APT_PRIO_INFO," Codec: %s\n", codec->name);
            apt_log(RECOG_LOG_MARK,APT_PRIO_INFO," Sample Rate: %d\n", codec->sampling_rate);
            apt_log(RECOG_LOG_MARK,APT_PRIO_INFO," Channels: %d\n", codec->channel_count);
            // Set up decoder/processing according to these parameters
        }
    }
}
*/

/** Open engine channel (asynchronous response MUST be sent)*/
static apt_bool_t demo_recog_channel_open(mrcp_engine_channel_t *channel)
{
  if(channel->attribs) {
    /* process attributes */
    const apr_array_header_t *header = apr_table_elts(channel->attribs);
    apr_table_entry_t *entry = (apr_table_entry_t *)header->elts;
    int i;
    for(i=0; i<header->nelts; i++) {
      apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Attrib name [%s] value [%s]",entry[i].key,entry[i].val);
    }
  }
  /*process_mrcp_channel(channel);*/

  return demo_recog_msg_signal(DEMO_RECOG_MSG_OPEN_CHANNEL,channel,NULL);
}

/** Close engine channel (asynchronous response MUST be sent)*/
static apt_bool_t demo_recog_channel_close(mrcp_engine_channel_t *channel)
{
  return demo_recog_msg_signal(DEMO_RECOG_MSG_CLOSE_CHANNEL,channel,NULL);
}

/** Process MRCP channel request (asynchronous response MUST be sent)*/
static apt_bool_t demo_recog_channel_request_process(mrcp_engine_channel_t *channel, mrcp_message_t *request)
{
  return demo_recog_msg_signal(DEMO_RECOG_MSG_REQUEST_PROCESS,channel,request);
}

/** Process RECOGNIZE request */
static apt_bool_t demo_recog_channel_recognize(mrcp_engine_channel_t *channel, mrcp_message_t *request, mrcp_message_t *response)
{
  /* process RECOGNIZE request */
  mrcp_recog_header_t *recog_header;
  demo_recog_channel_t *recog_channel = channel->method_obj;
  const mpf_codec_descriptor_t *descriptor = mrcp_engine_sink_stream_codec_get(channel);

  if(!descriptor) {
    apt_log(RECOG_LOG_MARK,APT_PRIO_WARNING,"Failed to Get Codec Descriptor " APT_SIDRES_FMT, MRCP_MESSAGE_SIDRES(request));
    response->start_line.status_code = MRCP_STATUS_CODE_METHOD_FAILED;
    return FALSE;
  }

  recog_channel->timers_started = TRUE;

  /* get recognizer header */
  recog_header = mrcp_resource_header_get(request);
  if(recog_header) {
    if(mrcp_resource_header_property_check(request,RECOGNIZER_HEADER_START_INPUT_TIMERS) == TRUE) {
      apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Received START_INPUT_TIMERS header");
      recog_channel->timers_started = recog_header->start_input_timers;
    }
    if(mrcp_resource_header_property_check(request,RECOGNIZER_HEADER_NO_INPUT_TIMEOUT) == TRUE) {
      mpf_activity_detector_noinput_timeout_set(recog_channel->detector,recog_header->no_input_timeout);
    }
    if(mrcp_resource_header_property_check(request,RECOGNIZER_HEADER_SPEECH_COMPLETE_TIMEOUT) == TRUE) {
      mpf_activity_detector_silence_timeout_set(recog_channel->detector,recog_header->speech_complete_timeout);
    }
  }

  if(!recog_channel->audio_out) {
    const apt_dir_layout_t *dir_layout = channel->engine->dir_layout;
    char *file_name = apr_psprintf(channel->pool,"utter-%dkHz-%s.pcm",
              descriptor->sampling_rate/1000,
              request->channel_id.session_id.buf);
    char *file_path = apt_vardir_filepath_get(dir_layout,file_name,channel->pool);
    if(file_path) {
      apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Open Utterance Output File [%s] for Writing",file_path);
      recog_channel->audio_out = fopen(file_path,"wb");
      if(!recog_channel->audio_out) {
        apt_log(RECOG_LOG_MARK,APT_PRIO_WARNING,"Failed to Open Utterance Output File [%s] for Writing",file_path);
      }
    }
  }

  /* Establish WebSocket connection to ASR backend */
  if(!recog_channel->ws_connected) {
    apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Establishing WebSocket connection to ASR backend %s:%s%s",
      recog_channel->asr_backend_url, recog_channel->asr_backend_port, recog_channel->asr_backend_path);

    if(!demo_recog_ws_connect(recog_channel)) {
      apt_log(RECOG_LOG_MARK,APT_PRIO_WARNING,"Failed to connect to ASR backend");
      response->start_line.status_code = MRCP_STATUS_CODE_METHOD_FAILED;
      return FALSE;
    }
  }

  response->start_line.request_state = MRCP_REQUEST_STATE_INPROGRESS;
  /* send asynchronous response */
  mrcp_engine_channel_message_send(channel,response);
  recog_channel->recog_request = request;
  return TRUE;
}

/** Process STOP request */
static apt_bool_t demo_recog_channel_stop(mrcp_engine_channel_t *channel, mrcp_message_t *request, mrcp_message_t *response)
{
  /* process STOP request */
  demo_recog_channel_t *recog_channel = channel->method_obj;
  /* store STOP request, make sure there is no more activity and only then send the response */
  recog_channel->stop_response = response;
  recog_channel->input_finished = TRUE;
  return TRUE;
}

/** Process START-INPUT-TIMERS request */
static apt_bool_t demo_recog_channel_timers_start(mrcp_engine_channel_t *channel, mrcp_message_t *request, mrcp_message_t *response)
{
  demo_recog_channel_t *recog_channel = channel->method_obj;
  recog_channel->timers_started = TRUE;
  return mrcp_engine_channel_message_send(channel,response);
}

/** Dispatch MRCP request */
static apt_bool_t demo_recog_channel_request_dispatch(mrcp_engine_channel_t *channel, mrcp_message_t *request)
{
  apt_bool_t processed = FALSE;
  mrcp_message_t *response = mrcp_response_create(request,request->pool);
  switch(request->start_line.method_id) {
    case RECOGNIZER_SET_PARAMS:
      break;
    case RECOGNIZER_GET_PARAMS:
      break;
    case RECOGNIZER_DEFINE_GRAMMAR:
      break;
    case RECOGNIZER_RECOGNIZE:
      apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Received RECOGNIZE event");
      processed = demo_recog_channel_recognize(channel,request,response);
      break;
    case RECOGNIZER_GET_RESULT:
      break;
    case RECOGNIZER_START_INPUT_TIMERS:
      apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Received START_INPUT_TIMERS event");
      processed = demo_recog_channel_timers_start(channel,request,response);
      break;
    case RECOGNIZER_STOP:
      processed = demo_recog_channel_stop(channel,request,response);
      break;
    default:
      break;
  }
  if(processed == FALSE) {
    /* send asynchronous response for not handled request */
    mrcp_engine_channel_message_send(channel,response);
  }
  return TRUE;
}

/** Callback is called from MPF engine context to destroy any additional data associated with audio stream */
static apt_bool_t demo_recog_stream_destroy(mpf_audio_stream_t *stream)
{
  return TRUE;
}

/** Callback is called from MPF engine context to perform any action before open */
static apt_bool_t demo_recog_stream_open(mpf_audio_stream_t *stream, mpf_codec_t *codec)
{
  return TRUE;
}

/** Callback is called from MPF engine context to perform any action after close */
static apt_bool_t demo_recog_stream_close(mpf_audio_stream_t *stream)
{
  return TRUE;
}

/* Raise demo START-OF-INPUT event */
static apt_bool_t demo_recog_start_of_input(demo_recog_channel_t *recog_channel)
{
  /* create START-OF-INPUT event */
  mrcp_message_t *message = mrcp_event_create(
            recog_channel->recog_request,
            RECOGNIZER_START_OF_INPUT,
            recog_channel->recog_request->pool);
  if(!message) {
    return FALSE;
  }

  /* set request state */
  message->start_line.request_state = MRCP_REQUEST_STATE_INPROGRESS;
  /* send asynch event */
  return mrcp_engine_channel_message_send(recog_channel->channel,message);
}

/* Load demo recognition result */
static apt_bool_t demo_recog_result_load(demo_recog_channel_t *recog_channel, mrcp_message_t *message)
{
  FILE *file;
  mrcp_engine_channel_t *channel = recog_channel->channel;
  const apt_dir_layout_t *dir_layout = channel->engine->dir_layout;
  char *file_path = apt_datadir_filepath_get(dir_layout,"result.xml",message->pool);
  if(!file_path) {
    return FALSE;
  }

  /* read the demo result from file */
  file = fopen(file_path,"r");
  if(file) {
    mrcp_generic_header_t *generic_header;
    char text[1024];
    apr_size_t size;
    size = fread(text,1,sizeof(text),file);
    apt_string_assign_n(&message->body,text,size,message->pool);
    fclose(file);

    /* get/allocate generic header */
    generic_header = mrcp_generic_header_prepare(message);
    if(generic_header) {
      /* set content types */
      apt_string_assign(&generic_header->content_type,"application/x-nlsml",message->pool);
      mrcp_generic_header_property_add(message,GENERIC_HEADER_CONTENT_TYPE);
    }
  }
  return TRUE;
}

static void write_nlsml_result(const char* recognized_text, char* buffer, size_t buflen)
{
    // Format NLSML string
    snprintf(buffer, buflen,
        "<?xml version=\"1.0\"?>\n"
        "<nlsml xmlns=\"http://www.w3.org/2001/06/grammar\">\n"
        "  <result>\n"
        "    <interpretation>\n"
        "      <input mode=\"speech\">%s</input>\n"
        "    </interpretation>\n"
        "  </result>\n"
        "</nlsml>\n",
        recognized_text ? recognized_text : ""
    );
}

/* Raise demo RECOGNITION-COMPLETE event */
static apt_bool_t demo_recog_recognition_complete(demo_recog_channel_t *recog_channel, mrcp_recog_completion_cause_e cause, const char *result_text)
{
  mrcp_recog_header_t *recog_header;
  /* create RECOGNITION-COMPLETE event */
  mrcp_message_t *message = mrcp_event_create(
            recog_channel->recog_request,
            RECOGNIZER_RECOGNITION_COMPLETE,
            recog_channel->recog_request->pool);
  if(!message) {
    return FALSE;
  }

  /* get/allocate recognizer header */
  recog_header = mrcp_resource_header_prepare(message);
  if(recog_header) {
    /* set completion cause */
    recog_header->completion_cause = cause;
    mrcp_resource_header_property_add(message,RECOGNIZER_HEADER_COMPLETION_CAUSE);
  }
  /* set request state */
  message->start_line.request_state = MRCP_REQUEST_STATE_COMPLETE;

  if(cause == RECOGNIZER_COMPLETION_CAUSE_SUCCESS) {
    int32_t result_text_len = result_text ? (int32_t)strlen(result_text) : 0;
    char nlsml_result[256 + result_text_len];
    write_nlsml_result(result_text, nlsml_result, sizeof(nlsml_result));
    apt_string_assign(&message->body, nlsml_result, message->pool);

    apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Compelet result : %s\n", nlsml_result);


    /* Set content type for the result */
    mrcp_generic_header_t *generic_header = mrcp_generic_header_prepare(message);
    if(generic_header) {
      /* set content types */
      apt_string_assign(&generic_header->content_type,"application/x-nlsml",message->pool);
      mrcp_generic_header_property_add(message,GENERIC_HEADER_CONTENT_TYPE);
    }
  }

  recog_channel->recog_request = NULL;
  /* send asynch event */
  return mrcp_engine_channel_message_send(recog_channel->channel,message);
}

/** Callback is called from MPF engine context to write/send new frame */
static apt_bool_t demo_recog_stream_write(mpf_audio_stream_t *stream, const mpf_frame_t *frame)
{
  demo_recog_channel_t *recog_channel = stream->obj;

  /* Get codec descriptor */
  const mpf_codec_descriptor_t *descriptor = mrcp_engine_sink_stream_codec_get(
      recog_channel->channel);
  /*
  if(descriptor) {
      apt_log(RECOG_LOG_MARK, APT_PRIO_INFO, "Audio Codec: %s, Rate: %d, Channels: %d, payload type: %d",
          descriptor->name.buf,
          descriptor->sampling_rate,
          descriptor->channel_count,
          descriptor->payload_type);
  }
  */

  if(recog_channel->stop_response) {
    /* send asynchronous response to STOP request */
    /*mrcp_engine_channel_message_send(recog_channel->channel,recog_channel->stop_response);*/
    /*recog_channel->stop_response = NULL;*/
    /*recog_channel->recog_request = NULL;*/
    return TRUE;
  }

  if(recog_channel->recog_request && !recog_channel->input_finished) {
    /*
    mpf_detector_event_e det_event = mpf_activity_detector_process(recog_channel->detector,frame);
    switch(det_event) {
      case MPF_DETECTOR_EVENT_ACTIVITY:
        apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Detected Voice Activity " APT_SIDRES_FMT,
          MRCP_MESSAGE_SIDRES(recog_channel->recog_request));
        demo_recog_start_of_input(recog_channel);
        break;
      case MPF_DETECTOR_EVENT_INACTIVITY:
        apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Detected Voice Inactivity " APT_SIDRES_FMT,
          MRCP_MESSAGE_SIDRES(recog_channel->recog_request));
        demo_recog_recognition_complete(recog_channel,RECOGNIZER_COMPLETION_CAUSE_SUCCESS);
        break;
      case MPF_DETECTOR_EVENT_NOINPUT:
        apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Detected Noinput " APT_SIDRES_FMT,
          MRCP_MESSAGE_SIDRES(recog_channel->recog_request));
        if(recog_channel->timers_started == TRUE) {
          demo_recog_recognition_complete(recog_channel,RECOGNIZER_COMPLETION_CAUSE_NO_INPUT_TIMEOUT);
        }
        break;
      default:
        break;
    }
    */

    if(recog_channel->recog_request) {
      if((frame->type & MEDIA_FRAME_TYPE_EVENT) == MEDIA_FRAME_TYPE_EVENT) {
        if(frame->marker == MPF_MARKER_START_OF_EVENT) {
          apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Detected Start of Event " APT_SIDRES_FMT " id:%d",
            MRCP_MESSAGE_SIDRES(recog_channel->recog_request),
            frame->event_frame.event_id);
        }
        else if(frame->marker == MPF_MARKER_END_OF_EVENT) {
          apt_log(RECOG_LOG_MARK,APT_PRIO_INFO,"Detected End of Event " APT_SIDRES_FMT " id:%d duration:%d ts",
            MRCP_MESSAGE_SIDRES(recog_channel->recog_request),
            frame->event_frame.event_id,
            frame->event_frame.duration);
        }
      }
    }

    if(recog_channel->audio_out) {
      fwrite(frame->codec_frame.buffer,1,frame->codec_frame.size,recog_channel->audio_out);
    }

    /* Stream audio to WebSocket ASR backend */
    if(recog_channel->ws_connected && frame->codec_frame.size > 0) {
      demo_recog_ws_send_audio(recog_channel, frame->codec_frame.buffer, frame->codec_frame.size);
    }
  }
  return TRUE;
}

static apt_bool_t demo_recog_msg_signal(demo_recog_msg_type_e type, mrcp_engine_channel_t *channel, mrcp_message_t *request)
{
  apt_bool_t status = FALSE;
  demo_recog_channel_t *demo_channel = channel->method_obj;
  demo_recog_engine_t *demo_engine = demo_channel->demo_engine;
  apt_task_t *task = apt_consumer_task_base_get(demo_engine->task);
  apt_task_msg_t *msg = apt_task_msg_get(task);
  if(msg) {
    demo_recog_msg_t *demo_msg;
    msg->type = TASK_MSG_USER;
    demo_msg = (demo_recog_msg_t*) msg->data;

    demo_msg->type = type;
    demo_msg->channel = channel;
    demo_msg->request = request;
    status = apt_task_msg_signal(task,msg);
  }
  return status;
}

static apt_bool_t demo_recog_msg_process(apt_task_t *task, apt_task_msg_t *msg)
{
  demo_recog_msg_t *demo_msg = (demo_recog_msg_t*)msg->data;
  switch(demo_msg->type) {
    case DEMO_RECOG_MSG_OPEN_CHANNEL:
      /* open channel and send asynch response */
      mrcp_engine_channel_open_respond(demo_msg->channel,TRUE);
      break;
    case DEMO_RECOG_MSG_CLOSE_CHANNEL:
    {
      /* close channel, make sure there is no activity and send asynch response */
      demo_recog_channel_t *recog_channel = demo_msg->channel->method_obj;
      if(recog_channel->audio_out) {
        fclose(recog_channel->audio_out);
        recog_channel->audio_out = NULL;
      }

      mrcp_engine_channel_close_respond(demo_msg->channel);
      break;
    }
    case DEMO_RECOG_MSG_REQUEST_PROCESS:
      demo_recog_channel_request_dispatch(demo_msg->channel,demo_msg->request);
      break;
    default:
      break;
  }
  return TRUE;
}

/* WebSocket protocols */
static const char *ws_protocol_name = "asr-protocol";
static struct lws_protocols protocols[] = {
  {
    .name = "asr-protocol",
    .callback = demo_recog_ws_callback,
    .per_session_data_size = sizeof(demo_recog_channel_t*),
    .rx_buffer_size = 4096,
  },
  { NULL, NULL, 0, 0 } /* terminator */
};

/* --- WebSocket service thread --- */
static void *ws_asr_service_thread(void *arg) {
  demo_recog_channel_t *client = (demo_recog_channel_t *)arg;
  while (TRUE) {
    lws_service(client->ws_context, 50);
  }
  return NULL;
}

/** WebSocket callback function */
static int demo_recog_ws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len)
{
  demo_recog_channel_t **channel_ptr = (demo_recog_channel_t**)user;
  demo_recog_channel_t *recog_channel = (channel_ptr && *channel_ptr) ? *channel_ptr : NULL;

  switch (reason) {
    case LWS_CALLBACK_CLIENT_ESTABLISHED:
      apt_log(RECOG_LOG_MARK, APT_PRIO_INFO, "WebSocket connection established");
      if(recog_channel) {
        recog_channel->ws_connected = TRUE;
      }
      char json_str[128];
      snprintf(json_str, sizeof(json_str), "{\"sample_rate\":%d}", 8000);

      unsigned char buf[LWS_PRE + 128];
      size_t len = strlen(json_str);
      memcpy(&buf[LWS_PRE], json_str, len);
      lws_write(wsi, &buf[LWS_PRE], len, LWS_WRITE_TEXT);
      break;

    case LWS_CALLBACK_CLIENT_RECEIVE:
      if(recog_channel && in && len > 0) {
        char *json_msg = apr_palloc(recog_channel->channel->pool, len + 1);
        memcpy(json_msg, in, len);
        json_msg[len] = '\0';
        demo_recog_process_asr_response(recog_channel, json_msg);
      }
      break;

    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
    case LWS_CALLBACK_CLIENT_CLOSED:
      apt_log(RECOG_LOG_MARK, APT_PRIO_INFO, "WebSocket connection closed/error");
      if(recog_channel) {
        recog_channel->ws_connected = FALSE;
        recog_channel->ws_connection = NULL;
      }
      break;

    case LWS_CALLBACK_CLIENT_WRITEABLE:
      /* Handle queued audio data */
      if(recog_channel && recog_channel->audio_queue_mutex) {
        apr_thread_mutex_lock(recog_channel->audio_queue_mutex);
        if(!APR_RING_EMPTY(&recog_channel->audio_queue, demo_recog_audio_buffer_t, link)) {
          demo_recog_audio_buffer_t *buffer = APR_RING_FIRST(&recog_channel->audio_queue);
          APR_RING_REMOVE(buffer, link);
          apr_thread_mutex_unlock(recog_channel->audio_queue_mutex);

          short *p_short = (short *)buffer->data;
          size_t num_samples = buffer->size / sizeof(short);
          float fsamples[num_samples];
          for (size_t i = 0; i < num_samples; i++) {
            fsamples[i] = p_short[i] / 32768.0f; // Normalize to [-1, 1]
          }

          unsigned char buf[LWS_PRE + num_samples * sizeof(float)];
          memcpy(&buf[LWS_PRE], fsamples, num_samples * sizeof(float));
          int ret = lws_write(wsi, buf + LWS_PRE, num_samples * sizeof(float),
                    LWS_WRITE_BINARY);

          /*int ret = lws_write(wsi, (unsigned char*)buffer->data, buffer->size, LWS_WRITE_BINARY);*/
          if (ret < 0) {
            apt_log(RECOG_LOG_MARK, APT_PRIO_WARNING, "Failed to write audio data to WebSocket");
            return -1;
          }
          lws_callback_on_writable(wsi);
        } else {
          if (recog_channel->input_finished) {
            // 1. Create the JSON object
            struct json_object *jobj = json_object_new_object();
            json_object_object_add(jobj, "event", json_object_new_string("end"));

            // 2. Serialize to string
            const char *msg = json_object_to_json_string(jobj);

            // 3. Send message over websocket
            size_t msg_len = strlen(msg);
            unsigned char buf[LWS_PRE + 1024];
            unsigned char *p = &buf[LWS_PRE];

            memcpy(p, msg, msg_len);

            lws_write(wsi, p, msg_len, LWS_WRITE_TEXT);

            // 4. Clean up
            json_object_put(jobj);

          }
          apr_thread_mutex_unlock(recog_channel->audio_queue_mutex);
        }
      }
      break;

    default:
      break;
  }

  return 0;
}

/** Establish WebSocket connection to ASR backend */
static apt_bool_t demo_recog_ws_connect(demo_recog_channel_t *recog_channel)
{

  struct lws_context_creation_info info;
  struct lws_client_connect_info connect_info;

  memset(&info, 0, sizeof(info));
  info.port = CONTEXT_PORT_NO_LISTEN;  
  info.protocols = protocols;

  recog_channel->ws_context = lws_create_context(&info);
  if(!recog_channel->ws_context) {
    apt_log(RECOG_LOG_MARK, APT_PRIO_ERROR, "Failed to create WebSocket context");
    return FALSE;
  }

  memset(&connect_info, 0, sizeof(connect_info));
  connect_info.context = recog_channel->ws_context;
  connect_info.address = recog_channel->asr_backend_url;
  connect_info.port = atoi(recog_channel->asr_backend_port);
  connect_info.path = recog_channel->asr_backend_path;
  connect_info.host = recog_channel->asr_backend_url;
  connect_info.origin = recog_channel->asr_backend_url;
  connect_info.protocol = ws_protocol_name;
  connect_info.userdata = &recog_channel;

  recog_channel->ws_connection = lws_client_connect_via_info(&connect_info);
  if(!recog_channel->ws_connection) {
    apt_log(RECOG_LOG_MARK, APT_PRIO_ERROR, "Failed to initiate WebSocket connection");
    lws_context_destroy(recog_channel->ws_context);
    recog_channel->ws_context = NULL;
    return FALSE;
  }

  pthread_create(&recog_channel->thread, NULL, ws_asr_service_thread, recog_channel);
  return TRUE;
}

/** Disconnect WebSocket connection */
static apt_bool_t demo_recog_ws_disconnect(demo_recog_channel_t *recog_channel)
{
  if(recog_channel->ws_connection) {
    lws_close_reason(recog_channel->ws_connection, LWS_CLOSE_STATUS_NORMAL, "recognition complete", 0);
    recog_channel->ws_connection = NULL;
  }

  if(recog_channel->ws_context) {
    lws_context_destroy(recog_channel->ws_context);
    recog_channel->ws_context = NULL;
  }

  recog_channel->ws_connected = FALSE;
  return TRUE;
}

/** Send audio data to WebSocket */
static apt_bool_t demo_recog_ws_send_audio(demo_recog_channel_t *recog_channel, const void *data, apr_size_t size)
{
  if(!recog_channel->ws_connected || !recog_channel->ws_connection) {
    return FALSE;
  }

  /* Queue audio data for sending */
  demo_recog_audio_buffer_t *buffer = apr_palloc(recog_channel->channel->pool, sizeof(demo_recog_audio_buffer_t));
  buffer->data = apr_palloc(recog_channel->channel->pool, size);
  memcpy(buffer->data, data, size);
  buffer->size = size;

  apr_thread_mutex_lock(recog_channel->audio_queue_mutex);
  APR_RING_INSERT_TAIL(&recog_channel->audio_queue, buffer, demo_recog_audio_buffer_t, link);
  apr_thread_mutex_unlock(recog_channel->audio_queue_mutex);

  lws_callback_on_writable(recog_channel->ws_connection);

  return TRUE;
}

/** Process ASR response from WebSocket */
static apt_bool_t demo_recog_process_asr_response(demo_recog_channel_t *recog_channel, const char *json_msg)
{
  json_object *root = json_tokener_parse(json_msg);
  if(!root) {
    apt_log(RECOG_LOG_MARK, APT_PRIO_WARNING, "Failed to parse JSON response: %s", json_msg);
    return FALSE;
  }

  json_object *type_obj, *result_obj, *endpoint_obj;
  if(!json_object_object_get_ex(root, "type", &type_obj) ||
     !json_object_object_get_ex(root, "text", &result_obj) ||
     !json_object_object_get_ex(root, "endpoint", &endpoint_obj)) {
    apt_log(RECOG_LOG_MARK, APT_PRIO_WARNING, "Invalid ASR response format: %s", json_msg);
    json_object_put(root);
    return FALSE;
  }

  const char *type_str = json_object_get_string(type_obj);
  const char *result_str = json_object_get_string(result_obj);

  if(strcmp(type_str, "partial") == 0) {
    /* Send INTERMEDIATE-RESULT event */
    demo_recog_intermediate_result(recog_channel, result_str);
  } else if(strcmp(type_str, "final") == 0) {
    /* Send RECOGNITION-COMPLETE event */
    apt_log(RECOG_LOG_MARK, APT_PRIO_INFO, "RECEIVE Final.");
    demo_recog_recognition_complete(recog_channel, RECOGNIZER_COMPLETION_CAUSE_SUCCESS, result_str);

    if(recog_channel->stop_response) {
        mrcp_engine_channel_message_send(recog_channel->channel,recog_channel->stop_response);
        recog_channel->stop_response = NULL;
    }
  }

  json_object_put(root);

  return TRUE;
}

/** Send INTERMEDIATE-RESULT event */
static apt_bool_t demo_recog_intermediate_result(demo_recog_channel_t *recog_channel, const char *result_text)
{
  if(!recog_channel->recog_request) {
    return FALSE;
  }

  /* create INTERMEDIATE-RESULT event */
  mrcp_message_t *message = mrcp_event_create(
            recog_channel->recog_request,
            RECOGNIZER_INTERMEDIATE_RESULT,
            recog_channel->recog_request->pool);
  if(!message) {
    return FALSE;
  }

  /* Set the result text as message body */
  if(result_text && strlen(result_text) > 0) {
    int32_t result_text_len = result_text ? (int32_t)strlen(result_text) : 0;
    char nlsml_result[256 + result_text_len];
    write_nlsml_result(result_text, nlsml_result, sizeof(nlsml_result));
    apt_string_assign(&message->body, nlsml_result, message->pool);

    /* Set content type for the result */
    mrcp_generic_header_t *generic_header = mrcp_generic_header_prepare(message);
    if(generic_header) {
      apt_string_assign(&generic_header->content_type, "application/x-nlsml", message->pool);
      mrcp_generic_header_property_add(message, GENERIC_HEADER_CONTENT_TYPE);
    }
  }

  /* set request state */
  message->start_line.request_state = MRCP_REQUEST_STATE_INPROGRESS;

  /* send asynch event */
  return mrcp_engine_channel_message_send(recog_channel->channel, message);
}
