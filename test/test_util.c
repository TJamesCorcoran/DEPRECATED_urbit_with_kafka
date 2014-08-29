// test/test_util.c
//
// This file is in the public domain.
//

#include "test_main.h"
#include "test_util.h"

int write_done = 0;
int test_done  = 1;


void util_gate_read(uv_timer_t* handle)
{
  printf("READ GATE: READY\n");
  write_done = 1;
  uv_timer_stop(handle);
}

void util_gate_done(uv_timer_t* handle)
{
  printf("FINISH GATE: DONE\n");
  test_done = 1;
  uv_timer_stop(handle);
}

void util_run_inside_loop(void (*func_ptr)(), void * data)
{

  uv_async_t * async_u = malloc(sizeof(uv_async_t));
  async_u->data = data;

  c3_w ret_w  = uv_async_init(u2_Host.lup_u, 
                              async_u, 
                              func_ptr );
  if (ret_w < 0){
    fprintf(stderr, "test: async_init fail\n");
    c3_assert(0);
  }

  ret_w  = uv_async_send(async_u);
  if (ret_w < 0){
    fprintf(stderr, "test: async_send fail\n");
    c3_assert(0);
  }
}

// funct_ptr - function to run
// data      -
// first     - ms to first invocation
// thereafter - ms after first invocation
void util_run_after_timer(void (*func_ptr)(uv_timer_t* handle), 
                          void * data, 
                          c3_d first_d, 
                          c3_d thereafter_d)
{
  uv_timer_t * timer_u = (uv_timer_t *) malloc (sizeof(uv_timer_t));
  timer_u->data = data;
  
  int ret = uv_timer_init(u2_Host.lup_u, timer_u);
  if (ret <0){
    fprintf(stderr, "error init timer\n");
    exit(-1);
  }

  ret = uv_timer_start(timer_u,
                       func_ptr,
                       first_d      * 1000,
                       thereafter_d * 1000
                       );

  if (ret <0){
    fprintf(stderr, "error init start\n");
    exit(-1);
  }

}

void util_loop_run()
{
  // head into event loop
  if ( u2_no == u2_Host.ops_u.bat ) {
    uv_run(u2L, UV_RUN_DEFAULT);
  }
}

void util_loop_stop()
{
  if (test_done < 1){
    printf("ENDING TEST: developer overrode test_done, so not ending yet!\n");
    return;
  }
  printf("ENDING TESTS\n");
  u2_lo_bail(u2_Host.arv_u);
  uv_stop(u2_Host.lup_u);
}

void _util_system_hardcore(char * cmd_str)
{
  if ( 0 != system(cmd_str) ) {
    fprintf(stderr, "could not %s\n", cmd_str);
    exit(-1);
  }
}

// start kafka & zookeeper
//
void util_kafka_run()
{
  bool verbose = false;
  char * redirect_str = verbose ? "" : "2>&1 > /dev/null";

  // clear the decks
  util_kafka_stop();
  sleep(5);

  // where?
  char * kafka_dir_c;
  kafka_dir_c = getenv("KAFKA_DIR");
  if (NULL == kafka_dir_c){
    fprintf(stderr, "ERROR: must install http://kafka.apache.org/downloads.html and set env variable KAFKA_DIR\n");
    fprintf(stderr, "e.g. '~/bus/urbit/kafka-0.8.1.1-src'\n");
    exit(-1);
  }

  // take off and nuke the log files from  orbit
  //
  char nuke_str[2048];
  sprintf(nuke_str ,"cd %s; rm -rf logs/* /tmp/zookeeper /tmp/kafka-logs", kafka_dir_c);
  _util_system_hardcore(nuke_str);
  _util_system_hardcore("rm -rf /tmp/kafka-logs/");

  // start zookeeper first
  //
  char zookeeper_start_str[2048];
  sprintf(zookeeper_start_str, "(cd %s; bin/zookeeper-server-start.sh config/zookeeper.properties)  %s &", kafka_dir_c, redirect_str);
  _util_system_hardcore(zookeeper_start_str);


  // let zookeeper get running before we start kafka
  //
  sleep(15);
  char kafka_start_str[2048];
  sprintf(kafka_start_str ,"(cd %s; bin/kafka-server-start.sh config/server.properties)  %s &", kafka_dir_c, redirect_str);
  _util_system_hardcore(kafka_start_str);


}

// stop kafka & zookeeper
//
void util_kafka_stop()
{
  char * kill_str = "ps ax | grep java | grep -v grep  | awk '{print $1}' | xargs kill -9";
  _util_system_hardcore(kill_str);
}
