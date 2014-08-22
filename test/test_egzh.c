#include "test_main.h"

#include <sys/time.h>

//--------------------
// egzh minifile work queue test
//
//--------------------
static void _test_egz_queue()
{
  void _enqueue(c3_d new_d,  c3_y msgtype_y);
  c3_t _dequeue(c3_d * ret_d,  c3_y * msgtype_y);

  // this inits mutex...and also starts consolidator thread (which we do NOT want)
  // what's our solution to test just consolidator?  ignore for now.
  u2_egz_init();

  _enqueue(10, 0);
  _enqueue(20, 0);
  _enqueue(30, 0);
  c3_t ret;
  c3_d number;
  c3_y msgtype_y;

  ret = _dequeue(&number, & msgtype_y);
  if (! (ret == c3_true && number == 10)){ printf("FAIL - egz_queue 1\n"); exit(-1);}

  ret = _dequeue(&number, & msgtype_y);
  if (! (ret == c3_true && number == 20)){ printf("FAIL - egz_queue 2\n"); exit(-1);}

  ret = _dequeue(&number, & msgtype_y);
  if (! (ret == c3_true && number == 30)){ printf("FAIL - egz_queue 3\n"); exit(-1);}

  // expect failure here
  ret = _dequeue(&number, & msgtype_y);
  if (! (ret == c3_false)){ printf("FAIL - egz_queue 4\n"); exit(-1);}

  printf("PASS - egz_queue\n");
}

void test_egz_queue_setup()
{
  _sist_home(u2A);
  u2_Host.arv_u->key = 1;

  setup_loop();
  util_run_inside_loop( & _test_egz_queue, NULL );
  util_run_after_timer( & util_end_test, NULL, 4 * 1000, 0);
  util_run_loop();
}

// test that minilog files get written.
// run the consolidator by hand.
// test just raw bytes
//
void _egz_consolidator(void *arg);
static void _test_egz_bytes()
{

  // delete any old files
  c3_w ret_w = system("rm -rf ~/urb/test");
  if (ret_w < 0) { fprintf(stderr, "failure to setup\n"); exit(-1); }


  // create the directory for the minifiles
  _sist_zest(u2A);

  // start the consolidator
  u2_egz_init();

  // write
  //
  c3_y * msg_1_y= (c3_y * ) "hello world 1";
  c3_w   len_1_w = strlen( (char * )msg_1_y);
  u2_egz_push(msg_1_y, len_1_w, 1, 0);

  c3_y * msg_2_y= (c3_y * ) "hello world 2";
  c3_w   len_2_w = strlen( (char * )msg_2_y);
  u2_egz_push(msg_2_y, len_2_w, 2, 0);

  c3_y * msg_3_y= (c3_y * ) "hello world 3";
  c3_w   len_3_w = strlen( (char * )msg_3_y);
  u2_egz_push(msg_3_y, len_3_w, 3, 0);

  // read
  sleep(15);
  
  u2_egz_pull_start();

  c3_d ent_d;
  c3_w  len_w;
  c3_y* bob_y;
  c3_y  msg_type_y;

  printf("BROKEN - egz_queue - design change: we log raw bytes w/o header, thus can't read them back :-( \n");
  return;

  c3_t ret_t =  u2_egz_pull_one(& ent_d,
                                & msg_type_y,
                                & len_w,
                                & bob_y);
  printf("ret_t = %i\n", ret_t);
  printf("ent_d = %lli\n", (long long int) ent_d);
  printf("len_w = %i\n", len_w);
  printf("msg_type_y = %i\n", msg_type_y);
  printf("msg = %s\n", bob_y);

  ret_t =  u2_egz_pull_one(& ent_d,
                           & msg_type_y,
                           & len_w,
                           & bob_y);

  printf("ret_t = %i\n", ret_t);
  printf("ent_d = %lli\n", (long long int) ent_d);
  printf("len_w = %i\n", len_w);
  printf("msg_type_y = %i\n", msg_type_y);
  printf("msg = %s\n", bob_y);

  ret_t =  u2_egz_pull_one(& ent_d,
                           & msg_type_y,
                           & len_w,
                           & bob_y);

  printf("ret_t = %i\n", ret_t);
  printf("ent_d = %lli\n", (long long int) ent_d);
  printf("len_w = %i\n", len_w);
  printf("msg_type_y = %i\n", msg_type_y);
  printf("msg = %s\n", bob_y);

}

void test_egz_bytes_setup()
{
  setup_loop();
  util_run_inside_loop( & _test_egz_bytes, NULL );
  util_run_after_timer( & util_end_test, NULL, 10 * 1000, 0);
  util_run_loop();

}

//--------------------
// egz write/read test
//
// architecture:
//    * we write minifiles quickly
//    * we have a full running system w a consolidator thread that runs every 10 seconds
//    * ...but single stepping means that it could take 10 minutes to get egz.hope written
//    * so we have a variable 'write_done' that will be set by util_read_gate() after 15s
//    * ...but a human in the debugger can also set write_done

static void _test_egz_ovo_w()
{
  u2_Host.arv_u->key = 1;

  u2_egz_write_header(u2_Host.arv_u, 0);

  char * payload_str = "egz ovo";
  printf("input was: %s\n", payload_str);

  u2_noun aaa = u2_ci_string(payload_str);  

  c3_d num_d = u2_egz_push_ova(u2A, aaa, LOG_MSG_PRECOMMIT);
  printf("ovo w: num_d = %llu\n", (unsigned long long int) num_d);
}


static void _test_egz_ovo_r(uv_timer_t* handle, int status)
{
  bool verbose = false;

  // no need for consolidator in this test
  //
  //  u2_egz_init();

  printf("test_egz_ova_r - read gate will open, but human can set write_done = 1 to proceed\n");
  if (0 == write_done){    return;  }

  printf("******** OVO R **********\n");
  
  u2_egz_pull_start();

  c3_d    ent_d;
  c3_y    msg_type_y;
  u2_noun ovo;

  c3_t ret_t =  u2_egz_pull_one_ova(& ent_d, & msg_type_y, & ovo);

  if (verbose){
    printf("ret_t = %i\n", ret_t);
    printf("ent_d = %lli\n", (long long int) ent_d);
    printf("msg_type_y = %i\n", msg_type_y);
    printf("payload = %s\n", u2_cr_string(ovo));
  }
  
  c3_c * ovo_str = u2_cr_string(ovo);

  printf("payload = %s\n", ovo_str);
  if (0 == strcmp((char *) ovo_str, "egz ovo")){
    printf("PASS - egz_ovo\n");
  } else {
    printf("FAIL - egz_ovo\n");
  }
}


void test_egz_ova_setup()
{

  _sist_home(u2A);
  u2_Host.arv_u->key = 1;


  u2_egz_rm();
  u2_egz_init();

  setup_loop();
  util_run_inside_loop( & _test_egz_ovo_w , NULL );
  util_run_after_timer( & _test_egz_ovo_r , NULL, READ_INTERVAL_SECONDS, READ_INTERVAL_SECONDS );
  util_run_after_timer( & util_read_gate , NULL, 3 * 1000, 1 );
  util_run_after_timer( & util_end_test  , NULL, 6 * 1000, 0);
  util_run_loop();
} 

//----------------------------------------
//----------------------------------------
//----------------------------------------

#define NUM_MESSAGES 1000
c3_d total_count_d = 0; 
c3_d total_bytes_d = 0;
struct timeval  before_tv;
struct timeval  after_cons_tv;

void _test_egz_stress_cb(c3_d count_d, c3_d newbytes_d)
{
  total_count_d += count_d;
  total_bytes_d += newbytes_d;

  if (total_count_d>= 2* NUM_MESSAGES){
    gettimeofday(&after_cons_tv, NULL);
    int delta_ms = (after_cons_tv.tv_sec - before_tv.tv_sec ) * 1000 * 1000 + (after_cons_tv.tv_usec - before_tv.tv_usec );
    delta_ms = delta_ms / 1000;
    printf("egzh CB: %llu events in %i ms\n", (unsigned long long int) total_count_d, delta_ms);
  } else {
    printf("egzh CB: %llu events\n", (unsigned long long int) total_count_d);
  }

}

static void _test_egz_stress_w()
{

  gettimeofday(&before_tv, NULL);

  int ii;
  for (ii = 0; ii < NUM_MESSAGES; ii++){
    char payload_str[1024];
    sprintf(payload_str, "stress: %i", ii);

    u2_noun aaa = u2_ci_string(payload_str);  

    c3_d num_d = u2_egz_push_ova(u2A, aaa, LOG_MSG_PRECOMMIT);
    printf("ovo w: num_d = %llu\n", (unsigned long long int) num_d);
  }
  ii --;

  struct timeval  after_send_tv;
  gettimeofday(&after_send_tv, NULL);
  int delta_ms = (after_send_tv.tv_sec - before_tv.tv_sec ) * 1000 * 1000 + (after_send_tv.tv_usec - before_tv.tv_usec );
  delta_ms = delta_ms / 1000;
  printf("%i writes in %i ms\n", ii + 1, delta_ms);

}

void test_egz_stress()
{

  _sist_home(u2A);
  u2_Host.arv_u->key = 1;
  u2_egz_rm();
  u2_egz_write_header(u2_Host.arv_u, 0);

  u2_egz_init();

  // get a callback after each consolidator run; gather stats
  u2_egz_set_consolidator_cb(_test_egz_stress_cb);

  setup_loop();
  util_run_inside_loop( & _test_egz_stress_w , NULL );
  //  util_run_after_timer( & test_egz_ovo_r , NULL, READ_INTERVAL_SECONDS, READ_INTERVAL_SECONDS );
  //  util_run_after_timer( & util_read_gate , NULL, 3 * 1000, 1 );
  //  util_run_after_timer( & util_end_test  , NULL, 6 * 1000, 0);
  util_run_loop();
} 
