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

  u2_kafk_pre_read(start_kafka_offset);

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
  u2_kafk_pre_read(test_kafka_logging_ova_rw_start);

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
