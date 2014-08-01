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

/**
 * Message delivery report callback using the richer rd_kafka_message_t object.
 */
static void _kafka_msg_delivered_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) 
{
  clock_t after = clock();

  float diff = ((float)after - (float)before) / (CLOCKS_PER_SEC * 1000);   
  // printf("-----------\n");
  // printf("before : %f\n",(float) before);   
  // printf("after : %f\n", (float) after);   
  // printf("duration : %f\n",diff);   


  if (rkmessage->err){
    fprintf(stderr, "%% Kafka message delivery failed: %s\n", rd_kafka_message_errstr(rkmessage));
  }
  else {
    //fprintf(stderr, "%% Kafka message delivery success (%zd bytes, offset %"PRId64")\n", rkmessage->len, rkmessage->offset);
  }
        
}

void u2_kafk_init()
{
  if (! u2_Host.ops_u.kaf_c) { return ; }

  rd_kafka_conf_t *conf_u;
  rd_kafka_topic_conf_t *topic_conf_u;
  char errstr[512];

  conf_u = rd_kafka_conf_new();
  topic_conf_u = rd_kafka_topic_conf_new();


  // Set up a message delivery report callback.
  // It will be called once for each message, either on successful
  // delivery to broker, or upon failure to deliver to broker. 
  rd_kafka_topic_conf_set(topic_conf_u,
                          "produce.offset.report",
                          "true", errstr, sizeof(errstr));
  rd_kafka_conf_set_dr_msg_cb(conf_u, _kafka_msg_delivered_cb);

  // Create Kafka handle
  if (!(u2K->kafka_handle_u = rd_kafka_new(RD_KAFKA_PRODUCER, conf_u,
                          errstr, sizeof(errstr)))) {
    fprintf(stderr,
            "%% Failed to create new producer: %s\n",
            errstr);
    exit(1);
  }

  // Add brokers.  These were specified on the command line.
  if (rd_kafka_brokers_add(u2K->kafka_handle_u, u2_Host.ops_u.kaf_c) == 0) {
    fprintf(stderr, "%% No valid brokers specified\n");
    exit(1);
  }

  // Create topic (remove tilda)
  u2K->topic_handle_u = rd_kafka_topic_new(u2K->kafka_handle_u, u2_Host.cpu_c + 1, topic_conf_u);
 
}

// 
// ent_c - 
void u2_kafk_pre_read(c3_c  start_offset_c)
{

  int64_t  offset_d = (int64_t) start_offset_c;

  if (rd_kafka_consume_start(u2K->topic_handle_u, 
                             RD_KAFKA_PARTITION_UA,
                             offset_d ) == -1){
    fprintf(stderr, "%% Failed to start consuming: %s\n",
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
    exit(1);
  }
}

void _kafk_consume(rd_kafka_message_t *rkmessage, void *opaque)
{
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      fprintf(stderr,
              "%% Consumer reached end of %s [%"PRId32"] "
              "message queue at offset %"PRId64"\n",
              rd_kafka_topic_name(rkmessage->rkt),
              rkmessage->partition, rkmessage->offset);

      return;
    }

    fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
            "offset %"PRId64": %s\n",
            rd_kafka_topic_name(rkmessage->rkt),
            rkmessage->partition,
            rkmessage->offset,
            rd_kafka_message_errstr(rkmessage));
    return;
  }

  fprintf(stdout, "%% Message (offset %"PRId64", %zd bytes):\n", rkmessage->offset, rkmessage->len);

  if (rkmessage->key_len) {
      printf("Key: %.*s\n",
             (int)rkmessage->key_len, (char *)rkmessage->key);
  }

  printf("%.*s\n", (int)rkmessage->len, (char *)rkmessage->payload);
}

u2_noun u2_kafk_read(u2_reck* rec_u,  u2_bean *  ohh)
{
  int run = 1;

  while (run) {
    rd_kafka_message_t *rkmessage;

    /* Consume single message.
     * See rdkafka_performance.c for high speed
     * consuming of messages. */
    rkmessage = rd_kafka_consume(u2K->topic_handle_u, RD_KAFKA_PARTITION_UA, 1000);

    if (!rkmessage) /* timeout */
      continue;

    _kafk_consume(rkmessage, NULL);

    /* Return message to rdkafka */
    rd_kafka_message_destroy(rkmessage);
  }

  /* Stop consuming */
  rd_kafka_consume_stop(u2K->topic_handle_u, RD_KAFKA_PARTITION_UA);

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

c3_d u2_kafk_push(c3_w kafk_rawlen_w, c3_w *   kafk_raw_w)
{
  if (! u2_Host.ops_u.kaf_c) {    return ; }

  // get sequence number
  // // NOTFORCHECKIN - DO SOMETHING WITH THIS
  c3_d seq_d = u2A->ent_d++;

  // Send message. 
  before = clock();

  u2_kafk_msg_header header_u;
  header_u.kafka_msg_format_version_y = 1;
  header_u.kafka_msg_type_y = 0;
  header_u.reserved_3_y = 0;
  header_u.reserved_4_y = 0;

  c3_w     kafk_len_w = kafk_len_w + sizeof(u2_kafk_msg_header);
  c3_w *   kafk_msg_w = malloc(kafk_len_w);
  if (kafk_msg_w == NULL){
    fprintf(stderr, "malloc failure\n");
    exit(1);

  }

  memcpy(kafk_msg_w, & header_u, sizeof(header_u));
  memcpy(kafk_msg_w + sizeof(header_u), kafk_raw_w, kafk_rawlen_w);

  if (rd_kafka_produce(u2K->topic_handle_u, 
                       RD_KAFKA_PARTITION_UA,
                       RD_KAFKA_MSG_F_COPY,
                       /* Payload and length */
                       kafk_msg_w,
                       kafk_len_w,
                       /* Optional key and its length */
                       NULL, 0,
                       /* Message opaque, provided in
                        * delivery report callback as
                        * msg_opaque. */
                       NULL) == -1) {
    fprintf(stderr,
            "%% Failed to produce to topic %s partition %i: %s\n",
            rd_kafka_topic_name(u2K->topic_handle_u), RD_KAFKA_PARTITION_UA,
            rd_kafka_err2str(
                             rd_kafka_errno2err(errno)));
    rd_kafka_poll(u2K->kafka_handle_u, 0);
  }


  free(kafk_msg_w);

  clock_t after = clock();
  float diff = ((float)after - (float)before) / (CLOCKS_PER_SEC * 1000);   

  // printf("before : %f\n",(float) before);   
  // printf("after : %f\n", (float) after);   
  // printf("duration : %f\n",diff);   

  /* Wait for messages to be delivered */
  // NOTFORCHECKIN - do we want this here?  I think we want to rip it out
  while (rd_kafka_outq_len(u2K->kafka_handle_u) > 0) {
    rd_kafka_poll(u2K->kafka_handle_u, 100);
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
u2_kafk_push_ova(u2_reck* rec_u, u2_noun ovo)
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
  bid_d = u2_kafk_push(bob_w, len_w);

  u2z(ron);  
         
  return(bid_d);
}

void u2_kafka_down()
{
  if (! u2_Host.ops_u.kaf_c) { return ; }

  /* Destroy the handle */
  rd_kafka_destroy(u2K->kafka_handle_u);


  /* Let background threads clean up and terminate cleanly. */
  rd_kafka_wait_destroyed(2000);
}

void u2_kafka_log_to_kafka()
{

}

void u2_kafka_kafka_to_log()
{
  
}
