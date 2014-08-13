// common logging - shared by egzh and kafk

#ifndef CLOG

#define LOG_MSG_PRECOMMIT  0
#define LOG_MSG_POSTCOMMIT 1



// u2_clpr: clog event prefix (1 before each event)
//
// version 'h'
typedef struct {
  c3_w syn_w;                         //  must equal mug of address
  c3_d ent_d;                         //  event sequence number
  c3_w len_w;                         //  byte length of this event
  c3_w mug_w;                         //  mug of entry
  c3_y msg_type_y;                    //  0 = precommit; 1 = commit
  c3_w ver_c;                         //  a,b,c .... today: 'h'
  // REMOVE c3_w tem_w;               //  raft term of event
  // REMOVE c3_w typ_w;               //  type of event, %ra|%ov
} u2_clpr;

void u2_clog_write_prefix(u2_clpr * prefix_u, c3_d ent_d, c3_y msg_type_y, c3_w len_w, c3_y * data_y);

c3_t u2_clog_check_prefix(u2_clpr * prefix_u);


void u2_clog_o2b(u2_noun ovo,
                 c3_w *  malloc_w, 
                 c3_w *  len_w, 
                 c3_y ** data_y);


void u2_clog_b2o(c3_w   len_w, 
                 c3_y * data_y,
                 u2_noun * ovo);

#define CLOG
#endif
