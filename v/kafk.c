/* v/kafk.c
**
** This file is in the public domain.
*/

// there are two methods of storing events:
//    * egz.hope log file
//    * kafka log servers
// this code implementes function around the latter.


//  Design goals:
//  1) bc logging takes time, we want to log events as soon as we receive them
//  2) ...and process them IN PARALLEL w kafka servers chugging along
//  3) bc processing can either succeed or fail, we want to 
//       log a second time (a "commit" of sorts) when processing succeeds.
//       That way later playback won't assume that failed things actually changed state.
//  
//  INVARIANT: What is absolutely certain is that we *can't emit a response until the
//  event is logged*.  Ideally, we are trying to log it at the same time as
//  we're trying to compute it.
//
//  THOUGHT (unconfirmed by Curtis): egz.hope logging does not suffer
//  under this constraint logging to local disk is effectively
//  foolproof, and thus can use a simpler architecture.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <uv.h>

#include "all.h"
#include "v/vere.h"
#include "v/kafk.h"

#include <errno.h>
#include <time.h>   

clock_t before;

#define WRITE_PARTITION 0 // RD_KAFKA_PARTITION_UA
#define READ_PARTITION  0  
#define READ_TIMEOUT    (3 * 1000)
#define POLLER_SLEEP_SECONDS 1

static void _kafka_admin_conf_dump(rd_kafka_conf_t       * prod_hand_u,
                                   rd_kafka_conf_t       * cons_hand_u,
                                   rd_kafka_topic_conf_t * prod_conf_u, 
                                   rd_kafka_topic_conf_t * cons_conf_u);
static void _kafka_msg_delivered_cb (rd_kafka_t *rk, 
                                     const rd_kafka_message_t *rkmessage, 
                                     void *opaque);
static void _kafk_poller(void *arg);

//--------------------
//  init functions
//--------------------

void u2_kafk_init()
{
  

  if (! u2_Host.ops_u.kaf_c) { return ; }

  // set global vars
  //----------------
  u2K->largest_offset_seen_pstcom_ds = 0;
  u2K->largest_offset_seen_precom_ds = 0;

  // Create configurations
  //----------------------
  //
  char errstr[512];

  // 1) build configuration objects
  //
  rd_kafka_conf_t *prod_conf_u  = rd_kafka_conf_new();
  rd_kafka_conf_t *cons_conf_u  = rd_kafka_conf_new();

  rd_kafka_topic_conf_t * topic_prod_conf_u = rd_kafka_topic_conf_new();
  rd_kafka_topic_conf_t * topic_cons_conf_u = rd_kafka_topic_conf_new();

  // 2) tweak conf objects
  //

  // delivery callback for producer
  rd_kafka_conf_set_dr_msg_cb(prod_conf_u, & _kafka_msg_delivered_cb); 

  rd_kafka_conf_res_t prod_topic_u = rd_kafka_topic_conf_set(topic_prod_conf_u, "produce.offset.report", "true", errstr, sizeof(errstr));
  if (prod_topic_u != RD_KAFKA_CONF_OK){
    c3_assert(0);
  }

  // 2.5) OPT: dump configuration
  //
  if ( NULL != u2_Host.ops_u.adm_c && 
       strcmp(u2_Host.ops_u.adm_c, "kcnf") == 0){
    _kafka_admin_conf_dump(prod_conf_u,
                           cons_conf_u,
                           topic_prod_conf_u,
                           topic_cons_conf_u);

  }

  // 3) use conf objects to create kafka handles; now we're done w the conf objects
  //
  u2K->kafka_prod_handle_u = rd_kafka_new(RD_KAFKA_PRODUCER, prod_conf_u, errstr, sizeof(errstr));
  if (! u2K->kafka_prod_handle_u ) {
    fprintf(stdout, "%% Failed to create producer: %s\n", errstr);
    exit(1);
  }
  u2K->kafka_cons_handle_u = rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf_u, errstr, sizeof(errstr));
  if (! u2K->kafka_cons_handle_u ) {
    fprintf(stderr, "\r%% Failed to create consumer: %s\n", errstr);
    exit(1);
  }
  
  // 4) Add brokers.  These were specified on the command line.
  //
  if (rd_kafka_brokers_add(u2K->kafka_prod_handle_u, u2_Host.ops_u.kaf_c) == 0) {
    fprintf(stderr, "\r%% No valid brokers specified\n");
    exit(1);
  }
  if (rd_kafka_brokers_add(u2K->kafka_cons_handle_u, u2_Host.ops_u.kaf_c) == 0) {
    fprintf(stderr, "\r%% No valid brokers specified\n");
    exit(1);
  }

  // 5) use conf object to create topic handles - named after ship
  //
  u2K->topic_prod_handle_u = rd_kafka_topic_new(u2K->kafka_prod_handle_u, u2_Host.cpu_c + 1, topic_prod_conf_u);
  u2K->topic_cons_handle_u = rd_kafka_topic_new(u2K->kafka_cons_handle_u, u2_Host.cpu_c + 1, topic_cons_conf_u);
 

  // note in the global datastructure that all systems are green
  u2K->inited_t = c3_true;

  // create polling thread 
  //
  // note that this new thread is not the libuv
  // event loop and has nothing to do w the libuv event loop
  //
  int ret = uv_thread_create(& u2K->kafk_poller_thread_u,
                             & _kafk_poller,
                             NULL);
  if (ret < 0){
    fprintf(stderr, "\rkafk: poller not started\n");
    c3_assert(0);
  }

}

//--------------------
//  write functions
//--------------------

// This is invoked via uv_async_send(), so we know we're back in the libuv worker thread.
// Thus we have access to loom.
//
void _kafka_finalize_and_emit(uv_async_t* async_u)
{
  clog_thread_baton * baton_u = (clog_thread_baton *)  async_u->data;

  if (baton_u->msg_type_y != LOG_MSG_PRECOMMIT){
    fprintf(stderr, "\rkafk: ERROR: emit stage for event %lli which is not in PRECOMMIT - state machine panic\n", (long long int) baton_u->seq_d);
    c3_assert(0);
  }

  // emit here
  // NOTFORCHECKIN

  // after success we can log a second time
  //
  // this is a big of a hack: we're operating on data that sort of claims to be opaque to us.
  // ...but it's cool because We Know The Truth (tm)
  u2_clpr * header_u = (u2_clpr *) baton_u->bob_y;

  header_u -> msg_type_y = LOG_MSG_POSTCOMMIT;
  baton_u  -> msg_type_y = LOG_MSG_POSTCOMMIT;

  u2_kafk_push(baton_u->bob_y, 
               baton_u->len_w,
               baton_u);
}

// "yo dawg, I heard you like callbacks in your callbacks..."
//
// For throughput testing we'd like our test suite to get a callback from our kafka code when  
// a message is firmly lodged in the kafka server.  I.e. when the kafka callback fires.  Hence
// a kafka callback callback.  Kafkaesque, no?
//
kafk_cb_cb_t _kafk_cb_cb_u = NULL;
void u2_kafk_set_cb_cb(kafk_cb_cb_t kafk_cb_cb)
{
  _kafk_cb_cb_u = kafk_cb_cb;
}


// Message push callback
//
//    * gets invoked when our pushed message has been committed to the kafka server
//    * gets invoked in one of kafka's threads, which means we can't touch  loom memory
//
// What we do: 
//        * sanity check
//        * store the kafka offset in u2K.  u2K will get checkpointed as part of the running system,
//          so that on boot later we'll know what the last kafka offset the checkpoint file reflects,
//          and then we can start querying kafka for log entries >> there, to build further state
//
//       
//        
// 
static void _kafka_msg_delivered_cb (rd_kafka_t *rk, 
                                     const rd_kafka_message_t *rkmessage, 
                                     void *opaque) 
{
  c3_t verbose_t = c3_false;

  clog_thread_baton * baton_u = (clog_thread_baton * ) opaque;

  // 0) sanity check
  //
  if (rkmessage->err){
    fprintf(stderr, "kafk: CB delivery failure: %i = %s\n", 
            rkmessage->err,
            rd_kafka_message_errstr(rkmessage));
    fprintf(stderr, "      details: thr: %lu // k: %lli  // u: %lli  // t: %i \n", 
            uv_thread_self(),
            (long long int) rkmessage->offset, 
            (long long int) baton_u->seq_d, 
            (int) baton_u->msg_type_y);
    c3_assert(0);
  } else
    if (c3_true == verbose_t){
    printf("\rkafk: CB delivery success: thr: %lu // k: %lli  // u: %lli  // t: %i \n", 
           uv_thread_self(),
           (long long int) rkmessage->offset, 
           (long long int) baton_u->seq_d, 
           (int) baton_u->msg_type_y); 
  }

  // 1) OPTIONAL: call back
  if (_kafk_cb_cb_u){
    (*_kafk_cb_cb_u)(rkmessage->offset, baton_u->seq_d, baton_u->msg_type_y);
  }

  // 2) store this kafka offset in u2k global so that global state of app can know
  //    what message we have processed (for recovery, replay, etc.)
  //
  if (LOG_MSG_PRECOMMIT == baton_u -> msg_type_y) {
    if (rkmessage->offset >  u2K->largest_offset_seen_precom_ds){
      u2K->largest_offset_seen_precom_ds = rkmessage->offset;
    } 
  } else if (LOG_MSG_POSTCOMMIT == baton_u -> msg_type_y) {
    if (rkmessage->offset >  u2K->largest_offset_seen_pstcom_ds){
      u2K->largest_offset_seen_pstcom_ds = rkmessage->offset;
    } 
  }


  // 3) if there's more work to do, update the msg type
  //    otherwise tidy up and finish
  if (LOG_MSG_POSTCOMMIT == baton_u -> msg_type_y){
    if (c3_true == verbose_t){
      printf("\rkafk: CB : 2nd logging complete for event %lli\n", (long long int) baton_u->seq_d);
    }
    free(baton_u->bob_y);
    free(baton_u);
    return;
  }

  // 4) re-package the data, hand off to libuv worker thread 

  uv_async_t * async_u = malloc(sizeof(uv_async_t));

  c3_w ret_w = uv_async_init(u2_Host.lup_u, 
                             async_u, 
                             &_kafka_finalize_and_emit );
  if (ret_w < 0){
    fprintf(stderr, "kafk: unable to inject event into uv\n");
    c3_assert(0);
  }
  async_u->data = (void *) baton_u;

  ret_w =   uv_async_send(async_u);
  if (ret_w < 0){
    fprintf(stderr, "kafk: unable to inject event into uv\n");
    c3_assert(0);
  }
}


// Note:
//    * does NOT write a u2_clpr event header: this just writes bytes
//
// input:
//     * data    -
//     * datalen - 
//     * baton_u - ptr to data block that we'll hand from thread to thread so that we can 
//                 pick up data and emit, finalize later.
// return:
//     * sequence #
//
void u2_kafk_push(c3_y * msg_y, c3_w len_w, clog_thread_baton * baton_u)
{
  c3_t verbose_t = c3_false;

  if (u2K->inited_t != c3_true){ 
    fprintf(stdout, "\rkafk: must init first\n"); 
    c3_assert(0);
  }

  // send the message
  //
  if (rd_kafka_produce(u2K->topic_prod_handle_u,   // topic
                       WRITE_PARTITION,            // partition
                       RD_KAFKA_MSG_F_COPY,        // msg flags
                       msg_y, len_w,               // payload
                       NULL, 0,                    // OPTIONAL: message key
                       (void*) baton_u)            // opaque payload for callback
      == -1) {
    fprintf(stdout,
               "\rkafka: push : FAIL: produce to topic '%s' partition %i on thr %lu: %s (u: %lli  // t: %i)\n",
               rd_kafka_topic_name(u2K->topic_prod_handle_u), 
               WRITE_PARTITION,
               uv_thread_self(),
               rd_kafka_err2str(rd_kafka_errno2err(errno)),
               (long long int) baton_u->seq_d, 
               (int)           baton_u->msg_type_y
               );
    // rd_kafka_poll(u2K->kafka_prod_handle_u, 0);
  } else if (c3_true == verbose_t) {
    fprintf(stdout,
               "\rkafka: push : OK  : produce to topic '%s' partition %i on thr %lu: (u: %lli  // t: %i)\n",
               rd_kafka_topic_name(u2K->topic_prod_handle_u), 
               WRITE_PARTITION,
               uv_thread_self(),
               (long long int) baton_u->seq_d, 
               (int)           baton_u->msg_type_y
               );
    
  }

  

}

// copy-and-paste programming; see also u2_egz_push_ova()
//
// input:
//    * rec_u
//    * ovo
// return:
//    * id of log msg
c3_d
u2_kafk_push_ova(u2_reck* rec_u, u2_noun ovo, c3_y msg_type_y)
{

  c3_w   data_len_w;
  c3_y * data_y;

  c3_w   full_len_w;
  c3_y * full_y;


  // 1) convert noun into bytes & write clog header
  //
  c3_d seq_d = u2A->ent_d++;
  u2_clog_o2b(ovo, seq_d, msg_type_y,            // inputs
              &full_y, &full_len_w, & data_y, & data_len_w);  // outputs

  // 2) write a data baton for use in callback & final emit
  //
  clog_thread_baton * baton_u = (clog_thread_baton *) malloc(sizeof(clog_thread_baton));
  baton_u->bob_y         = full_y;
  baton_u->len_w         = full_len_w;
  baton_u->seq_d         = seq_d;
  baton_u->msg_type_y    = msg_type_y;
  baton_u->ovo           = ovo;
  memset(& baton_u->push_thread_u, 0 , sizeof(uv_thread_t));


  // 3) log
  //
  u2_kafk_push(full_y, full_len_w, baton_u);

  return(seq_d);
}

//--------------------
//  read functions
//--------------------


// Prepare for reading.   
// 
//    Call this once, specifying the offset - the KAFKA offset, not the ovum message number!
//
void u2_kafk_pull_start(c3_d offset_d)
{
  if (u2K->inited_t != c3_true){ 
    fprintf(stderr, "\rkafk: must init first\n"); 
    c3_assert(0);
  }

  if (rd_kafka_consume_start(u2K->topic_cons_handle_u, 
                             READ_PARTITION,
                             offset_d ) == -1){
    fprintf(stderr, "\r%% Failed to start consuming: %s\n", rd_kafka_err2str(rd_kafka_errno2err(errno)));
    c3_assert(0);
  }
}


// Read one kafka message and return the payload and some header info from it.
//
//
// Which one?  You don't get to specify - that falls out from where
// you started the consumption sequence via u2_kafk_pre_read()
//
c3_t u2_kafk_pull_one(c3_d * ent_d,            // return arg
                      c3_y * msg_type_y,       // return arg
                      c3_w * len_w,            // return arg
                      c3_y ** buf_y)           // return arg
            
{
  c3_t verbose_t = c3_false;

  if (u2K->inited_t != c3_true){ 
    fprintf(stderr, "\rkafk: must init first\n"); 
    c3_assert(0);
  }

  rd_kafka_message_t *rkmessage;

  // (1) Consume single message.
  //     See rdkafka_performance.c for high speed consuming of messages. 
  rkmessage = rd_kafka_consume(u2K->topic_cons_handle_u, 
                               READ_PARTITION, 
                               READ_TIMEOUT
                               );
  if (NULL == rkmessage){
    uL(fprintf(uH, "kafk_read() failed: %s\n", strerror(errno)));
    exit(1);
  } else if (c3_true == verbose_t) {
    if (c3_true == verbose_t){ printf("\rpull_one: k: %lli\n", (long long int) rkmessage->offset); }
  }

  // (2) check error messages
  if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    uL(fprintf(uH, "kafk error: %s\n", rd_kafka_err2str(rkmessage->err)));
    return(c3_false);
  }

  // (3) pull out our event prefix
  //
  if (rkmessage->len < sizeof(u2_clpr)){
    uL(fprintf(uH, "kafk error: too small for event prefix\n"));
    return(c3_false);
  }

  u2_clpr * clog_u = (u2_clpr *) rkmessage->payload;
  c3_t ret_t = u2_clog_check_prefix(clog_u);
  if (c3_true != ret_t){
    return(c3_false);
  }

  // (4) pull out our message
  *len_w = clog_u->len_w;
  *buf_y = (c3_y *) malloc(*len_w);
  memcpy(* buf_y, rkmessage->payload + sizeof(u2_clpr), *len_w);
  

  // success: return payload and metadata
  *ent_d            = clog_u->ent_d;
  *msg_type_y       = clog_u->msg_type_y;

  rd_kafka_message_destroy(rkmessage);

  return(c3_true);
}

// Read ova from kafka.
//
// input args:
//    * rec_u -
//    * ohh   - 
// output args:
//    * ohh   - ???
//
// return:
//    * first read noun, which is a cell structure, which means that later we can iterate over it by using
//      u2h() and u2t().  All reading from kafka should be done in this func!
//
c3_t u2_kafk_pull_one_ova(c3_d    * ent_d,  
                          c3_y    * msg_type_y,
                          u2_noun * ovo)
{
  if (u2K->inited_t != c3_true){ 
    uL(fprintf(uH, "kafk: must init first\n")); 
    c3_assert(0);
  }

  // 4) pull from log
  c3_y * payload_y;
  c3_w payload_len_w;

  c3_t success=  u2_kafk_pull_one(ent_d,            
                                  msg_type_y, 
                                  & payload_len_w,
                                  & payload_y); 
  if (c3_false == success){
    return(c3_false);
  }

  u2_clog_b2o(payload_len_w, payload_y, ovo);

  return(c3_true);
}

// an experiment
c3_t u2_kafk_pull_batch_ova()
{
  rd_kafka_message_t * msgs_u;

  c3_w ret_w = rd_kafka_consume_batch(u2K->topic_cons_handle_u, 
                                      READ_PARTITION, 
                                      READ_TIMEOUT * 20,
                                      & msgs_u,
                                      20); // NOTFORCHECKIN
  if (ret_w < 0){
    printf("\rbatch: %s\n", rd_kafka_err2str(rd_kafka_errno2err(errno)));
  } else {
    printf("\rbatch: %i read\n", ret_w);
  }

  
  return(ret_w);
}

u2_noun  u2_kafk_pull_all(u2_reck* rec_u,  u2_bean *  ohh)
{
  u2_noun     roe = u2_nul;
  return(roe);
  // NOTFORCHECKIN - unimplemented
}

#define U2_KAFK_VERSION 1

void u2_kafk_commit()
{
// NOTFORCHECKIN
}

void u2_kafk_decommit()
{
  // NOTFORCHECKIN
}

// admin:
//   convert egz to kafka
void u2_kafka_admin_egz_to_kafka()
{
  uL(fprintf(uH, "u2_kafka_admin_egz_to_kafka() unimplemented"));
  exit(1);
}

// admin:
//   convert kafka to egz
void u2_kafka_admin_kafka_to_egz()
{
  uL(fprintf(uH, "u2_kafka_admin_kafka_to_egz() unimplemented"));
  exit(1);
}

static void _kafka_admin_strdump(char * title, const char ** conf_c, size_t cntp)
{
  printf("\r\n");
  printf("\r%s\n", title);
  printf("\r--------------------\n");
  int ii;
  for(ii = 0; ii < cntp; ii += 2){
    printf("\r   %-50s: %20s\n", conf_c[ii], conf_c[ii+1]);
  }

}
// admin:
//   dump kafka configuration
void _kafka_admin_conf_dump(rd_kafka_conf_t       * prod_hand_u,
                            rd_kafka_conf_t       * cons_hand_u,
                            rd_kafka_topic_conf_t * prod_topic_u, 
                            rd_kafka_topic_conf_t * cons_topic_u)
{
  size_t cntp;
  const char ** ret_c;

  ret_c = rd_kafka_conf_dump (prod_hand_u, &cntp);
  _kafka_admin_strdump("producer handle conf", ret_c, cntp);

  ret_c = rd_kafka_conf_dump (cons_hand_u, &cntp);
  _kafka_admin_strdump("consumer handle conf", ret_c, cntp);

  ret_c = rd_kafka_topic_conf_dump (prod_topic_u, &cntp);
  _kafka_admin_strdump("producer topic conf", ret_c, cntp);

  ret_c = rd_kafka_topic_conf_dump (cons_topic_u, &cntp);
  _kafka_admin_strdump("consumer topic conf", ret_c, cntp);

  exit(0);
}


static int _kafk_poller_run = 1;

void _kafk_poller(void *arg) 
{
  c3_t verbose_t = c3_false;

  if (verbose_t) { fprintf(stdout, "\rkafk: poller live\n"); }

  while (_kafk_poller_run) {

    if (verbose_t) { fprintf(stdout, "\rkafk: poller waking for %i poll events\n", 
                             rd_kafka_outq_len(u2K->kafka_prod_handle_u)); }

    while (rd_kafka_outq_len(u2K->kafka_prod_handle_u) > 0) {
      rd_kafka_poll(u2K->kafka_prod_handle_u, 
                    100                   // ms timeout
                    );
    }

    if (verbose_t) { fprintf(stdout, "\rkafk: poller sleeping\n"); }

    sleep(POLLER_SLEEP_SECONDS);

  } // while(1)

  if (verbose_t) { fprintf(stdout, "\rkafk: poller shutting down\n"); }
}

//--------------------
//  shutdown functions
//--------------------


void u2_kafk_shutdown()
{
  if (! u2_Host.ops_u.kaf_c) { return ; }

  c3_t verbose_t = c3_true;
  if (c3_true == verbose_t){     printf("\r********** KAFKA shutdown\n");   }

  _kafk_poller_run = 0;

  c3_w ret_w;
  ret_w = rd_kafka_consume_stop(u2K->topic_cons_handle_u, READ_PARTITION);
  if (ret_w < 0){
    fprintf(stderr, "kafk: error in rd_kafka_consume_stop %s\n", rd_kafka_err2str(rd_kafka_errno2err(errno)));
  }

  // Destroy the handles
  rd_kafka_destroy(u2K->kafka_prod_handle_u);
  rd_kafka_destroy(u2K->kafka_cons_handle_u);



  /* Let background threads clean up and terminate cleanly. */
  //  rd_kafka_wait_destroyed(2000);


}

