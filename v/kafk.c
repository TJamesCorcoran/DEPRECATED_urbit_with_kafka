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

#define WRITE_PARTITION RD_KAFKA_PARTITION_UA
#define READ_PARTITION  0  

static void _kafka_msg_delivered_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);

//--------------------
//  init functions
//--------------------

void u2_kafk_init()
{
  

  if (! u2_Host.ops_u.kaf_c) { return ; }

  // Create Kafka handles
  //----------------------
  //
  char errstr[512];

  // 1) build configuration objects
  rd_kafka_conf_t *prod_conf_u  = rd_kafka_conf_new();
  rd_kafka_conf_t *cons_conf_u  = rd_kafka_conf_new();

  // 2) tweak conf objects to set up delivery callback (producer only)
  rd_kafka_conf_set_dr_msg_cb(prod_conf_u, _kafka_msg_delivered_cb);

  // 3) use conf objects to create kafka handles
  u2K->kafka_prod_handle_u = rd_kafka_new(RD_KAFKA_PRODUCER, prod_conf_u, errstr, sizeof(errstr));
  if (! u2K->kafka_prod_handle_u ) {
    fprintf(stderr, "%% Failed to create producer: %s\n", errstr);
    exit(1);
  }
  u2K->kafka_cons_handle_u = rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf_u, errstr, sizeof(errstr));
  if (! u2K->kafka_cons_handle_u ) {
    fprintf(stderr, "%% Failed to create consumer: %s\n", errstr);
    exit(1);
  }

  // 2) Add brokers.  These were specified on the command line.
  if (rd_kafka_brokers_add(u2K->kafka_prod_handle_u, u2_Host.ops_u.kaf_c) == 0) {
    fprintf(stderr, "%% No valid brokers specified\n");
    exit(1);
  }
  if (rd_kafka_brokers_add(u2K->kafka_cons_handle_u, u2_Host.ops_u.kaf_c) == 0) {
    fprintf(stderr, "%% No valid brokers specified\n");
    exit(1);
  }



  // create topic handles
  //---------------------
  //

  //   1) create configuration
  rd_kafka_topic_conf_t * topic_prod_conf_u = rd_kafka_topic_conf_new();
  rd_kafka_topic_conf_t * topic_cons_conf_u = rd_kafka_topic_conf_new();

  //    2) customize configuration
  rd_kafka_conf_res_t prod_topic_u = rd_kafka_topic_conf_set(topic_prod_conf_u, "produce.offset.report", "true", errstr, sizeof(errstr));
  if (prod_topic_u != RD_KAFKA_CONF_OK){
    exit(-1);
  }

  //    3) actually create topic handles
  u2K->topic_prod_handle_u = rd_kafka_topic_new(u2K->kafka_prod_handle_u, u2_Host.cpu_c + 1, topic_prod_conf_u);
  u2K->topic_cons_handle_u = rd_kafka_topic_new(u2K->kafka_cons_handle_u, u2_Host.cpu_c + 1, topic_cons_conf_u);
 

  // note in the global datastructure that all systems are green
  u2K->inited_t = c3_true;
}

//--------------------
//  write functions
//--------------------

// Message push callback
//    gets invoked once our pushed message is in the system.
//    What we do: 
//        * sanity checking
//        * store the kafka offset in u2K.  u2K will get checkpointed as part of the running system,
//          so that on boot later we'll know what the last kafka offset the checkpoint file reflects,
//          and then we can start querying kafka for log entries >> there, to build further state
//        * more ovum processing (emitting side effects, etc.).  We can do this now bc we've FULLY digested
//          the ovum
// 
static void _kafka_msg_delivered_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) 
{
  if (rkmessage->err){
    fprintf(stderr, "kafk: CB delivery failure: %s\n", rd_kafka_message_errstr(rkmessage));
  }
  else {
    if (rkmessage->offset > u2K->largest_offset_seen_ds){
      fprintf(stderr, "kafk: OK     CB in order. Old: %lli ; New: %lli \n", (long long int) u2K->largest_offset_seen_ds,  (long long int) rkmessage->offset );      
    } else {
      fprintf(stderr, "kafk: WARN!! CB out of order. Old: %lli ; New: %lli \n", (long long int) u2K->largest_offset_seen_ds, (long long int)  rkmessage->offset );      
    }

    u2K->largest_offset_seen_ds = rkmessage->offset;

    printf("%% kafk: CB success (%zd bytes, offset %"PRId64")\n", rkmessage->len, rkmessage->offset);
  }
        
}



// input:
//     * data
//     * datalen
// return:
//     * sequence #
//
c3_d u2_kafk_push(c3_y * msg_y, c3_w len_w, c3_y msg_type_y)
{
  if (u2K->inited_t != c3_true){ 
    fprintf(stderr, "kafk: must init first\n"); 
    exit(-1);
  }

  // send the message
  //
  if (rd_kafka_produce(u2K->topic_prod_handle_u, 
                       WRITE_PARTITION,
                       RD_KAFKA_MSG_F_COPY,
                       /* Payload and length */
                       msg_y,
                       len_w,
                       /* Optional key and its length */
                       NULL, 0,
                       /* Message opaque, provided in
                        * delivery report callback as
                        * msg_opaque. */
                       NULL) == -1) {
    fprintf(stderr,
            "%% Failed to produce to topic %s partition %i: %s\n",
            rd_kafka_topic_name(u2K->topic_prod_handle_u), WRITE_PARTITION,
            rd_kafka_err2str(
                             rd_kafka_errno2err(errno)));
    rd_kafka_poll(u2K->kafka_prod_handle_u, 0);
  }

  // NOTFORCHECKIN - do we want this here?  I think we want to rip it out
  while (rd_kafka_outq_len(u2K->kafka_prod_handle_u) > 0) {
    rd_kafka_poll(u2K->kafka_prod_handle_u, 100);
  }
  
  return(u2A->ent_d);
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

  c3_w   malloc_w;   // space for header AND payload
  c3_w   len_w;      // length of payload
  c3_y * data_y;     

  // convert into bytes, w some padding up front for the header
  u2_clog_o2b(ovo, & malloc_w, & len_w, & data_y);

  // write the header in place
  u2_clog_write_prefix((u2_clpr *) data_y, u2A->ent_d++, msg_type_y, len_w, data_y + sizeof(u2_clpr));
  
  c3_d bid_d = u2_kafk_push( data_y, malloc_w, msg_type_y);


  return(bid_d);
}

//--------------------
//  read functions
//--------------------


// Prepare for reading.   
// 
//    Call this once, specifying the offset - the KAFKA offset, not the ovum message number!
//
void u2_kafk_pre_read(c3_d offset_d)
{
  if (u2K->inited_t != c3_true){ 
    fprintf(stderr, "kafk: must init first\n"); 
    exit(-1);
  }

  if (rd_kafka_consume_start(u2K->topic_cons_handle_u, 
                             READ_PARTITION,
                             offset_d ) == -1){
    fprintf(stderr, "%% Failed to start consuming: %s\n", rd_kafka_err2str(rd_kafka_errno2err(errno)));
    exit(-1);
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
  if (u2K->inited_t != c3_true){ 
    fprintf(stderr, "kafk: must init first\n"); 
    exit(-1);
  }

  rd_kafka_message_t *rkmessage;

  // (1) Consume single message.
  //     See rdkafka_performance.c for high speed consuming of messages. 
  rkmessage = rd_kafka_consume(u2K->topic_cons_handle_u, READ_PARTITION, 1000);
  if (NULL == rkmessage){
    fprintf(stderr, "kafk_read() failed: %s\n", strerror(errno));
    exit(1);
  }

  // (2) check error messages
  if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    fprintf(stderr, "kafk error: %s\n", rd_kafka_err2str(rkmessage->err));
    return(c3_false);
  }

  // (3) pull out our event prefix
  //
  if (rkmessage->len < sizeof(u2_clpr)){
    fprintf(stderr, "kafk error: too small for event prefix\n");
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
    fprintf(stderr, "kafk: must init first\n"); 
    exit(-1);
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

u2_noun  u2_kafk_pull_all(u2_reck* rec_u,  u2_bean *  ohh)
{
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

//--------------------
//  shutdown functions
//--------------------


void u2_kafka_down()
{
  if (! u2_Host.ops_u.kaf_c) { return ; }

  // Destroy the handles
  rd_kafka_destroy(u2K->kafka_prod_handle_u);
  rd_kafka_destroy(u2K->kafka_cons_handle_u);


  /* Let background threads clean up and terminate cleanly. */
  rd_kafka_wait_destroyed(2000);


}

// admin:
//   convert egz to kafka
void u2_kafka_admin_egz_to_kafka()
{
  fprintf(stderr, "u2_kafka_admin_egz_to_kafka() unimplemented");
  exit(1);
}

// admin:
//   convert kafka to egz
void u2_kafka_admin_kafka_to_egz()
{
  fprintf(stderr, "u2_kafka_admin_kafka_to_egz() unimplemented");
  exit(1);
}
