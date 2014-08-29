#include "test_main.h"
#include "test_util.h"

#define READ_INTERVAL_SECONDS (5 * 1000)

void test_kafka_logging_bytes()
{
  u2_Host.ops_u.kaf_c = strdup("localhost:9092");
  u2_kafk_init();

  clog_thread_baton baton_1_u;
  clog_thread_baton baton_2_u;

  c3_y * msg_1_c= (c3_y * ) "bytes one";
  u2_kafk_push(  msg_1_c, strlen( (char * )msg_1_c), & baton_1_u);
  while (rd_kafka_outq_len(u2K->kafka_prod_handle_u) > 0) {
    rd_kafka_poll(u2K->kafka_prod_handle_u, 100);
  }

  c3_ds start_kafka_offset = u2K->largest_offset_seen_pstcom_ds;

  c3_y * msg_2_c= (c3_y *) "bytes two";
  u2_kafk_push( msg_2_c, strlen( (char * )msg_2_c), & baton_2_u);
  while (rd_kafka_outq_len(u2K->kafka_prod_handle_u) > 0) {
    rd_kafka_poll(u2K->kafka_prod_handle_u, 100);
  }


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

void test_kafka_logging_ova_r(uv_timer_t* handle)
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
  util_run_after_timer( & util_gate_read           , NULL, 10 * 1000, 1 );
  util_run_after_timer( & util_loop_stop           , NULL, 20 * 1000, 0);
  util_loop_run();
}

//----------------------------------------
//----------------------------------------
//----------------------------------------

#define NUM_MESSAGES 1000

static c3_d     pre_count_d = 0; 
static c3_d     post_count_d = 0; 
struct timeval  before_tv;
struct timeval  after_tv;
struct timeval  commit_tv;

// this is an instrument we inject to modify the the normal behavior
// of kafk; it merely lets us gather statistics which would not
// otherwise be gathered.
//
void _test_kafk_stress_cb(c3_d kafk_off_d, c3_d seq_d, c3_y msg_type_y)
{
  if (msg_type_y == LOG_MSG_PRECOMMIT){
    pre_count_d ++;

    if (pre_count_d == NUM_MESSAGES){
      gettimeofday(&after_tv, NULL);
      int delta_ms = (after_tv.tv_sec - before_tv.tv_sec ) * 1000 * 1000 + (after_tv.tv_usec - before_tv.tv_usec );
      delta_ms = delta_ms / 1000;
      printf("kafk PRE final: %llu events in %i ms\n", (unsigned long long int) pre_count_d, delta_ms);
    } 

  } else {
    post_count_d ++;

    if (post_count_d == NUM_MESSAGES){// NOTFORCHECKIN
      gettimeofday(&commit_tv, NULL);
      int delta_ms = (commit_tv.tv_sec - before_tv.tv_sec ) * 1000 * 1000 + (commit_tv.tv_usec - before_tv.tv_usec );
      delta_ms = delta_ms / 1000;
      printf("\rkafk POST final: %llu events in %i ms\n", (unsigned long long int) post_count_d, delta_ms);
      
      // we're ready for the read portion of the test now
      //
      //      u2_kafk_shutdown();
      sleep(10);
      write_done = 1;
    }
  } 

}

static void _test_kafk_stress_w(uv_async_t * async_u)
{
  // a fine idea but not today: this sucks in the "garbage" message
  //   u2_kafk_pull_start(RD_KAFKA_OFFSET_END);

  c3_t verbose_t = c3_false;

  // do a garbage write, just to set
  //     u2K->largest_offset_seen_precom_ds;
  {
  uL(fprintf(uH, "******** STRESS GARBAGE ********** : writing garbage to establish read point begin\n"));
    u2_noun aaa = u2_ci_string("garbage");  
    c3_d num_d = u2_kafk_push_ova(u2A, aaa, LOG_MSG_PRECOMMIT);
    uL(fprintf(uH, "garbage ova # %lli\n", (long long int) num_d));


    // wait for a precommit kafka offset to be retrieved
    //
    // it'd be nice to get the postcommit message, but I've waited for up to 40 seconds and it doesn't arrive;
    // seems to need
    int ii = 0;
    while (0 == u2K->largest_offset_seen_precom_ds) {
      // printf("\r%i\n", ii);
      ii ++;
      sleep(1);
    }

    //    if (0 == u2K->largest_offset_seen_precom_ds)
    //        (0 == u2K->largest_offset_seen_pstcom_ds)) { 
    //      fprintf(stderr, 
    //              "\rTEST ERROR: garbage not seen; don't know proper offset %llu, %llu \n", 
    //              (long long unsigned) u2K->largest_offset_seen_precom_ds,
    //              (long long unsigned) u2K->largest_offset_seen_pstcom_ds);
    //      exit(-1);
    //    }


    test_kafka_logging_ova_rw_start = u2K->largest_offset_seen_precom_ds + 1;
    uL(fprintf(uH, "kafk logging will begin w offset # %lli\n", (long long int) test_kafka_logging_ova_rw_start));
  }

  uL(fprintf(uH, "******** STRESS WRITE ********** worker thread = %lu\n", uv_thread_self()  ));

  u2_kafk_set_cb_cb(_test_kafk_stress_cb);

  gettimeofday(&before_tv, NULL);



  // do the writing
  int ii;
  for (ii = 0; ii < NUM_MESSAGES; ii++){
    char payload_str[1024];
    sprintf(payload_str, "stress: %i", ii);

    u2_noun aaa = u2_ci_string(payload_str);  

    c3_d num_d = u2_kafk_push_ova(u2A, aaa, LOG_MSG_PRECOMMIT);


    if(c3_true == verbose_t) {  uL(fprintf(uH, "ovo w: num_d = %llu\n", (unsigned long long int) num_d)); }
  }

  uL(fprintf(uH, "******** STRESS WRITE DONE **********\n"));

  struct timeval  after_send_tv;
  gettimeofday(&after_send_tv, NULL);
  int delta_ms = (after_send_tv.tv_sec - before_tv.tv_sec ) * 1000 * 1000 + (after_send_tv.tv_usec - before_tv.tv_usec );
  delta_ms = delta_ms / 1000;
  uL(fprintf(uH, "%i writes in %i ms\n", ii, delta_ms));

  //   for (ii = 0; ii < 1000; ii++){
  //     char payload_str[1024];
  //     sprintf(payload_str, "padding: %i", ii);
  // 
  //     u2_noun aaa = u2_ci_string(payload_str);  
  // 
  //     c3_d num_d = u2_kafk_push_ova(u2A, aaa, LOG_MSG_PRECOMMIT);
  // 
  // 
  //   }
  // 
  //   uL(fprintf(uH, "******** PADDING WRITE DONE **********\n"));

}

c3_t u2_kafk_pull_batch_ova(); // NOTFORCHECKIN

static void _test_kafk_stress_r(uv_timer_t* handle)
{
  c3_t verbose_t = c3_false;


  if (0 == write_done){    return;  }
  uv_timer_stop(handle);    // we got through once; that's all that's needed

  fprintf(stdout, "\r******** STRESS READ ********** from k offset %lli\n", (long long int) test_kafka_logging_ova_rw_start);

  sleep(20);


  u2_kafk_pull_start(test_kafka_logging_ova_rw_start);
  //  u2_kafk_pull_batch_ova();

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
      fprintf(stderr, "\rread # %i: error\n", i);
      errors_w ++;
      continue;
    } else {
      if (c3_true == verbose_t){ fprintf(stdout, "\rread # %i: success\n", i); }
    }

    char * payload_str = u2_cr_string(ovo);
    if (c3_true == verbose_t){    fprintf(stdout, "\rread: %lli // %i // %s\n", (long long int) ent_d, msg_type_y, payload_str); }

    if (0 == strcmp("garbage", payload_str)){
      if (c3_true == verbose_t){      fprintf(stdout, "\rgarbage found; discarding\n"); }
      i --;
      continue;
    }

    int msg_number;
    if (sscanf(payload_str, "stress: %i", & msg_number) < 1){
      fprintf(stderr, "\rread # %i: sscanf problem!\n", i);
    }
    found[msg_number][msg_type_y] = strdup(payload_str);
    
  }

  // make sure we got the right strings

  char gold_str[2048];

  for (i = 0 ; i < 2;  i ++) {
    int j;
    // generate the gold
    sprintf(gold_str, "stress: %i", i); 

    for (j = 0; j < 1; j ++){
      if (NULL == found[i][j]) {
        fprintf(stderr, "\rerror: number %i, type %i error: NULL\n", i, j);
        errors_w ++;
      } else if (0 != strcmp(found[i][j], gold_str)){
        fprintf(stderr, "\rerror: number %i, type %i error. Expected %s got %s\n", i, j, gold_str, found[i][j]);
        errors_w ++;
      } else {
        // perfect!
      }
    }
  }
  
  if (0 == errors_w) {
    fprintf(stdout, "\r*** PERFECT! ****\n");
  } else {
    fprintf(stdout, "\nrerrors: %i\n", errors_w);
  }

  // shut down the test
  util_loop_stop(); 
}

void test_kafk_stress()
{
  //  uL(fprintf(uH, "***** about to start zookeeper & kafka server *****\n"));  
  //  util_kafka_run();
  uL(fprintf(uH, "***** servers should be running *****\n"));  

  // setup kafka use
  u2_Host.ops_u.kaf_c = strdup("localhost:9092");

  // create pier files
  _sist_home(u2A);

  u2_kafk_init();

  // set balls in motion
  util_run_inside_loop( & _test_kafk_stress_w, NULL );

  util_run_after_timer( & _test_kafk_stress_r , NULL, 10, 1 );


  util_loop_run();

  printf("\rtest done.\n");
} 

