#include "test_main.h"
#include "test_util.h"

#define READ_INTERVAL_SECONDS (5 * 1000)

void test_kafka_logging_bytes()
{
  u2_Host.ops_u.kaf_c = strdup("localhost:9092");
  u2_kafk_init();

  c3_y * msg_1_c= (c3_y * ) "bytes one";
  u2_kafk_push(  msg_1_c, strlen( (char * )msg_1_c), NULL);

  c3_y * msg_2_c= (c3_y *) "bytes two";
  u2_kafk_push( msg_2_c, strlen( (char * )msg_2_c), NULL);

  c3_ds start_kafka_offset = u2K->largest_offset_seen_ds;

  u2_kafk_pull_start(start_kafka_offset);

  c3_y * msg_c;
  c3_w len_w;
  c3_d ent_d;           
  c3_y msg_type_y;

  c3_t success = u2_kafk_pull_one(& ent_d, & msg_type_y, &len_w, & msg_c);
  if (success != c3_true){
    printf("error read 1");
  } else {
    printf("ent out:  %lli\n", (long long int) ent_d);
    printf("kfk type: %i\n",  msg_type_y);
    printf("msg len:  %i\n", len_w);
    printf("msg:      %s\n",  msg_c);
  }

  success = u2_kafk_pull_one(& ent_d, & msg_type_y, &len_w, & msg_c);
  if (success != c3_true){
    printf("error");
  } else {
    printf("ent out:  %lli\n", (long long int) ent_d);
    printf("kfk type: %i\n",  msg_type_y);
    printf("msg len:  %i\n", len_w);
    printf("msg:      %s\n",  msg_c);
  }

  exit(0);

}

c3_ds test_kafka_logging_ova_rw_start = 0;

void test_kafka_logging_ova_w()
{
  bool verbose = true;

  // write
  char * input_str = "double yolk";
  u2_noun aaa = u2_ci_string(input_str);  
  c3_d ent_1_d = u2_kafk_push_ova(u2_Host.arv_u,  aaa, LOG_MSG_PRECOMMIT);
  if (verbose){ printf("ent 1 #: %lli\n", (long long int) ent_1_d); }

  // what's the biggest offset seen before we start running this test? 
  // store it as a global var; we'll use it later
  test_kafka_logging_ova_rw_start = u2K->largest_offset_seen_precom_ds;


}

void test_kafka_logging_ova_r(uv_timer_t* handle, int status)
{
  bool verbose = true;

  //  printf("test_kafka_logging_ova_r - read gate will open, but human can set write_done = 1 to proceed\n");
  if (0 == write_done){    return;  }
  uv_timer_stop(handle);    // we got through once; that's all that's needed
  

  // prepare to read
  //
  u2_kafk_pull_start(test_kafka_logging_ova_rw_start);

  // precommit msg
  //
  c3_d    ent_d;
  c3_y    msg_type_y;
  u2_noun ova;
  c3_c*   output_str = NULL;

  c3_t success = u2_kafk_pull_one_ova(& ent_d, & msg_type_y, &ova);
  if (success != c3_true){
    printf("FAIL - error read 1"); return;
  }
  if (verbose) {
    printf("ent: %lli\n", (long long int) ent_d);
    printf("kfk type: %i\n",  msg_type_y);
    output_str = u2_cr_string(ova);
    printf("ova: %s\n",  output_str);
  }
  if (!(0 == strcmp("double yolk", output_str) && msg_type_y == LOG_MSG_PRECOMMIT)){
    printf("FAIL - error content 1:\n");
    exit(-1);
  }


  // postcommit msg
  //
  output_str = NULL;
  success = u2_kafk_pull_one_ova(& ent_d, & msg_type_y, &ova);
  if (success != c3_true){
    printf("FAIL - error read 2"); return;
  }
  if (verbose) {
    printf("ent: %lli\n", (long long int) ent_d);
    printf("kfk type: %i\n",  msg_type_y);
    output_str = u2_cr_string(ova);
    printf("ova: %s\n",  output_str);
  }
  if (!(0 == strcmp("double yolk", output_str) && msg_type_y == LOG_MSG_POSTCOMMIT)){
    printf("FAIL - error read 22\n");
    exit(-1);
  }


  printf("PASS - kafka_logging_ova \n");

}

void test_kafka_logging_ova_setup()
{
  u2_Host.ops_u.kaf_c = strdup("localhost:9092");
  u2_Host.arv_u->key = 1;
  u2_kafk_init();

  //  setup_loop();
  util_run_inside_loop( & test_kafka_logging_ova_w , NULL );
  util_run_after_timer( & test_kafka_logging_ova_r , NULL, READ_INTERVAL_SECONDS, READ_INTERVAL_SECONDS );
  util_run_after_timer( & util_gate_read           , NULL, 20 * 1000, 1 );
  util_run_after_timer( & util_loop_stop            , NULL, 30 * 1000, 0);
  util_loop_run();
}

//----------------------------------------
//----------------------------------------
//----------------------------------------

#define NUM_MESSAGES 10

static c3_d total_count_d = 0; 
static c3_d total_bytes_d = 0;
struct timeval  before_tv;
struct timeval  after_cons_tv;

// this is an instrument we inject to modify the the normal behavior
// of kafk; it merely lets us gather statistics which would not
// otherwise be gathered.
//
void _test_kafk_stress_cb(c3_d count_d, c3_d newbytes_d)
{
  total_count_d += count_d;
  total_bytes_d += newbytes_d;

  if (total_count_d>= 2* NUM_MESSAGES){
    gettimeofday(&after_cons_tv, NULL);
    int delta_ms = (after_cons_tv.tv_sec - before_tv.tv_sec ) * 1000 * 1000 + (after_cons_tv.tv_usec - before_tv.tv_usec );
    delta_ms = delta_ms / 1000;
    printf("kafk CB final: %llu events in %i ms\n", (unsigned long long int) total_count_d, delta_ms);
    u2_kafk_shutdown(); // mostly to keep stdout clean
  } else {
    printf("kafk CB part: %llu events\n", (unsigned long long int) total_count_d);
  }

}

static void _test_kafk_stress_w()
{

  gettimeofday(&before_tv, NULL);

  int ii;

  for (ii = 0; ii < NUM_MESSAGES; ii++){
    char payload_str[1024];
    sprintf(payload_str, "stress: %i", ii);

    u2_noun aaa = u2_ci_string(payload_str);  

    c3_d num_d = u2_kafk_push_ova(u2A, aaa, LOG_MSG_PRECOMMIT);
    // uL(fprintf(uH, "ovo w: num_d = %llu\n", (unsigned long long int) num_d));
  }
  ii --;

  struct timeval  after_send_tv;
  gettimeofday(&after_send_tv, NULL);
  int delta_ms = (after_send_tv.tv_sec - before_tv.tv_sec ) * 1000 * 1000 + (after_send_tv.tv_usec - before_tv.tv_usec );
  delta_ms = delta_ms / 1000;
  uL(fprintf(uH, "%i writes in %i ms\n", ii + 1, delta_ms));

}

static void _test_kafk_stress_r()
{
  bool verbose = false;

  uL(fprintf(uH, "test_kafk_ova_r - read gate will open, but human can set write_done = 1 to proceed\n"));
  if (0 == write_done){    return;  }

  uL(fprintf(uH, "******** STRESS READ **********\n"));
  
  u2_kafk_pull_start(1); // NOTFORCHECKIN - wrong arg

  char * found[NUM_MESSAGES][2];
  memset(found, 0, sizeof(found));

  c3_w errors_w = 0;
  int i;

  for (i = 0 ; i < 2 * NUM_MESSAGES;  i ++) {
    c3_d    ent_d;
    c3_y    msg_type_y;
    u2_noun ovo;

    c3_t ret_t =  u2_kafk_pull_one_ova(& ent_d, & msg_type_y, & ovo);
    if (c3_false == ret_t){
      uL(fprintf(uH, "error: read %i\n", i));
      errors_w ++;
      continue;
    }

    char * payload_str = u2_cr_string(ovo);
    if (verbose){
      uL(fprintf(uH, "ret_t = %i\n", ret_t));
      uL(fprintf(uH, "ent_d = %lli\n", (long long int) ent_d));
      uL(fprintf(uH, "msg_type_y = %i\n", msg_type_y));
      uL(fprintf(uH, "payload = %s\n", payload_str));
    }
    

    found[ent_d - 1][msg_type_y] = strdup(payload_str);
  }

  // make sure we got the right strings

  char gold_str[2048];

  for (i = 0 ; i < 2;  i ++) {
    int j;
    // generate the gold
    sprintf(gold_str, "stress: %i", i);

    for (j = 0; j < 1; j ++){
      if (NULL == found[i][j]) {
        uL(fprintf(uH, "error: number %i, type %i error: NULL\n", i, j));
        errors_w ++;
      } else if (0 != strcmp(found[i][j], gold_str)){
        uL(fprintf(uH, "error: number %i, type %i error. Expected %s got %s\n", i, j, gold_str, found[i][j]));
        errors_w ++;
      } else {
        // perfect!
      }
    }
  }
  
  if (0 == errors_w) {
    uL(fprintf(uH, "*** PERFECT! ****\n"));
  } else {
    uL(fprintf(uH, "errors: %i\n", errors_w));
  }

  // shut down the test
  util_loop_stop(); 
}

void test_kafk_stress()
{
  // create pier files
  _sist_home(u2A);

  // set a callback after each consolidator run to gather stats
  // then start consolidator
  u2_kafk_init();

  // set balls in motion
  //  util_run_inside_loop( & _test_kafk_stress_w , NULL );

  util_run_after_timer( & _test_kafk_stress_w     , NULL, 0, 0 );

  // util_run_after_timer( & util_gate_read     , NULL, , 0 );

  write_done = 1;
  util_run_after_timer( & _test_kafk_stress_r , NULL, 180, 5 );


  util_loop_run();

  uL(fprintf(uH, "test done.\n"));
} 

