// u2_kafk: kafka / egz.hope state
//

#include "rdkafka.h"  /* for Kafka driver */



// start all Kafka utilities
void u2_kafk_init(void);

// read
void u2_kafk_pre_read(c3_d  start_offset_c);                //
c3_t u2_kafk_read_one();     //
u2_noun u2_kafk_read_all(u2_reck* rec_u,  u2_bean *  ohh);  //

// write
c3_d u2_kafk_push(c3_w*   kafk_bob_w, c3_w kafk_len_w, c3_y msg_type_y);
c3_d u2_kafk_push_ova(u2_reck* rec_u, u2_noun ovo, c3_y msg_type_y);



// shutdown
void u2_kafka_down(void);



