#include <sys/stat.h>
#include <sys/types.h>

/* u2_uled: event log header.
*/
typedef struct {
  c3_l mag_l;                         //  mug of log format, 'a', 'b'...
  c3_w kno_w;                         //  kernel number validated with
  c3_l sal_l;                         //  salt for passcode
  c3_l key_l;                         //  mug of crypto key, or 0
  c3_l sev_l;                         //  host process identity
  c3_l tno_l;                         //  terminal count in host
} u2_uled;

/* u2_olar: event log trailer, old version.
*/
typedef struct {
  c3_w syn_w;                         //  must equal mug of address
  c3_w ent_w;                         //  event sequence number
  c3_w len_w;                         //  word length of this event
  c3_w mug_w;                         //  mug of entry
} u2_olar;

/* u2_ular: event log trailer.
*/
typedef struct {
  c3_w syn_w;                         //  must equal mug of address
  c3_d ent_d;                         //  event sequence number
  c3_w len_w;                         //  word length of this event
  c3_w mug_w;                         //  mug of entry
  c3_w tem_w;                         //  raft term of event
  c3_w typ_w;                         //  type of event, %ra|%ov
} u2_ular;


void     u2_egz_init();

void     u2_egz_write_header(u2_reck* rec_u, c3_l sal_l);
void     u2_egz_rewrite_header(u2_reck* rec_u,  c3_i fid_i, u2_bean ohh, u2_uled * led_u);

c3_t     u2_egz_open(u2_reck* rec_u, c3_i * fid_i, u2_uled * led_u);

u2_noun  u2_egz_read_all(u2_reck* rec_u,  c3_i fid_i,  u2_bean *  ohh);

c3_d     u2_egz_push(u2_raft* raf_u, c3_w* bob_w, c3_w len_w);
c3_d     u2_egz_push_ova(u2_reck* rec_u, u2_noun ovo);

