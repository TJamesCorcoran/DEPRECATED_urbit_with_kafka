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
    fprintf(stderr, "%% Failed to start consuming: %s\n",
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
    exit(1);
  }
}

// deal with kafka-specific details of reading: kafka error codes, etc.
// boil it down to two  things:
//   success: yes or no?
//   payload: via return args
//
c3_t _kafk_read_internal(rd_kafka_message_t *rkmessage, 
                         c3_y* buf_y, // return arg
                         c3_l* len_l,
                         c3_l  maxlen_l)
{
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      fprintf(stderr,
              "%% Consumer reached end of %s [%"PRId32"] "
              "message queue at offset %"PRId64"\n",
              rd_kafka_topic_name(rkmessage->rkt),
              rkmessage->partition, rkmessage->offset);

      return(c3_false);
    }

    fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
            "offset %"PRId64": %s\n",
            rd_kafka_topic_name(rkmessage->rkt),
            rkmessage->partition,
            rkmessage->offset,
            rd_kafka_message_errstr(rkmessage));
    return(c3_false);
  }

  fprintf(stdout, "%% Message (offset %"PRId64", %zd bytes):\n", rkmessage->offset, rkmessage->len);

  if (rkmessage->len > maxlen_l) {
    fprintf(stderr, "kafk: message from kafka log too big for read buffer");
    return (c3_false);
  }
  *len_l = rkmessage->len;
  memcpy(buf_y, rkmessage->payload, maxlen_l);

  return(c3_true);
}

// Read one kafka message and return the payload and some header info from it.
//
//
// Which one?  You don't get to specify - that falls out from where
// you started the consumption sequence via u2_kafk_pre_read()
//
c3_t u2_kafk_read_one(c3_d * ent_d,            // return arg
                      c3_y * kafka_msg_type_y, // return arg

                      c3_y * buf_c,            // return arg
                      c3_w * len_w,            // return arg
                      c3_w   maxlen_w)            
{
  if (u2K->inited_t != c3_true){ 
    fprintf(stderr, "kafk: must init first\n"); 
    exit(-1);
  }

  rd_kafka_message_t *rkmessage;

  // Consume single message.
  // See rdkafka_performance.c for high speed consuming of messages. 
  rkmessage = rd_kafka_consume(u2K->topic_cons_handle_u, READ_PARTITION, 1000);
  if (NULL == rkmessage){
    fprintf(stderr, "kafk_read() failed: %s\n", strerror(errno));
    exit(1);
  }

  c3_y raw_msg_y[2048];
  c3_w raw_msg_len;

  c3_t success = _kafk_read_internal(rkmessage, raw_msg_y, &raw_msg_len, 2048);
  if (success != c3_true){
    fprintf(stderr, "kafk_read() failed");
    exit(1);
  }

  rd_kafka_message_destroy(rkmessage);

  // POST-CONDITION: 
  //    raw_msg_y contains:
  //      * a u2_kafk_msg_header
  //      * the msg

  u2_kafk_msg_header header_u;
  memcpy(& header_u, raw_msg_y, sizeof(u2_kafk_msg_header));

  if (header_u.kafka_msg_format_version_y != 1){
    fprintf(stderr, "kafk: read gave version != 1");
    exit(1);
  }
  if ((header_u.kafka_msg_type_y != KAFK_MSG_PRECOMMIT) &&
      (header_u.kafka_msg_type_y != KAFK_MSG_POSTCOMMIT)) {
    fprintf(stderr, "kafk: illegal message type");
    exit(1);
  }

  c3_w actual_msg_len_w = raw_msg_len - sizeof(u2_kafk_msg_header);
  if (actual_msg_len_w  > maxlen_w){
    fprintf(stderr, "kafk: message payload exceeds bufffer size");
    exit(1);
  }

  // success: return payload and metadata
  *ent_d            = header_u.ent_d;
  *kafka_msg_type_y = header_u.kafka_msg_type_y;
  *len_w            = actual_msg_len_w;
  memcpy(buf_c, raw_msg_y + sizeof(u2_kafk_msg_header), actual_msg_len_w);

  return(c3_true);
}

u2_noun u2_kafk_read_all(u2_reck* rec_u,  u2_bean *  ohh)
{
  if (u2K->inited_t != c3_true){ 
    fprintf(stderr, "kafk: must init first\n"); 
    exit(-1);
  }


  int run = 1;


  while (run) {
    // NOTFORCHECKIN
  }

  // Stop consuming
  //     Note that we only consume at startup, so, yes, this is correct.
  //
  rd_kafka_consume_stop(u2K->topic_cons_handle_u, READ_PARTITION);

  exit(-1); // NOTFORCHECKIN - unimplemented!
  u2_noun uglyhack_u = (u2_noun) malloc(sizeof(u2_noun));
  return(uglyhack_u);
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
      fprintf(stderr, "kafk: OK     CB in order. Old: %lli ; New: %lli \n", u2K->largest_offset_seen_ds, rkmessage->offset );      
    } else {
      fprintf(stderr, "kafk: WARN!! CB out of order. Old: %lli ; New: %lli \n", u2K->largest_offset_seen_ds, rkmessage->offset );      
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
c3_d u2_kafk_push(c3_y * kafk_raw_y, c3_w kafk_rawlen_w, c3_y msg_type_y)
{
  if (u2K->inited_t != c3_true){ 
    fprintf(stderr, "kafk: must init first\n"); 
    exit(-1);
  }

  // prepend our header
  //
  u2_kafk_msg_header header_u;
  header_u.kafka_msg_format_version_y = 1;
  header_u.kafka_msg_type_y           = KAFK_MSG_PRECOMMIT;
  header_u.ent_d                      = u2A->ent_d++; // sequence number

  c3_w     payload_len_w = sizeof(u2_kafk_msg_header) + kafk_rawlen_w;
  c3_y *   payload_msg_y = malloc(payload_len_w);
  if (payload_msg_y == NULL){
    fprintf(stderr, "malloc failure: %s\n", strerror(errno));
    exit(1);

  }

  memcpy(payload_msg_y, & header_u, sizeof(header_u));
  memcpy(payload_msg_y + sizeof(header_u), kafk_raw_y, kafk_rawlen_w);

  // send the message
  //
  if (rd_kafka_produce(u2K->topic_prod_handle_u, 
                       WRITE_PARTITION,
                       RD_KAFKA_MSG_F_COPY,
                       /* Payload and length */
                       payload_msg_y,
                       payload_len_w,
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


  free(payload_msg_y);

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
  u2_noun ron;
  c3_d    bid_d;

  // serialize
  ron = u2_cke_jam(u2nc(u2k(rec_u->now), ovo));
  c3_assert(rec_u->key);

  // encrypt
  ron = u2_dc("en:crua", u2k(rec_u->key), ron);

  // copy data to raft_bob_w, manage ref counts
  c3_w    len_w;
  c3_w*   bob_w;

  len_w = u2_cr_met(5, ron);
  bob_w = c3_malloc(len_w * 4L);
  u2_cr_words(0, len_w, bob_w, ron);
  bid_d = u2_kafk_push( (c3_y *) bob_w, len_w, msg_type_y);

  u2z(ron);  
         
  return(bid_d);
}

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
