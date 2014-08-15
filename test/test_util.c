// test/test_util.c
//
// This file is in the public domain.
//

#include "test_main.h"
int write_done;

void util_end_test()
{
  printf("ENDING TESTS\n");
  u2_reck*   rec_u = u2_Host.arv_u;
  u2_lo_bail(rec_u);
}

void util_read_gate(uv_timer_t* handle, int status)
{
  printf("READ GATE: READY\n");
  write_done = 1;
  uv_timer_stop(handle);
}

void util_run_inside_loop(void (*func_ptr)(), void * data)
{

  uv_async_t * async_u = malloc(sizeof(uv_async_t));
  async_u->data = data;

  uv_async_init(u2_Host.lup_u, 
                async_u, 
                func_ptr );

  uv_async_send(async_u);
}

// funct_ptr - function to run
// data      -
// first     - ms to first invocation
// thereafter - ms after first invocation
void util_run_after_timer(void (*func_ptr)(uv_timer_t* handle, int status), void * data, c3_d first_d, c3_d thereafter_d)
{
  uv_timer_t * timer_u = (uv_timer_t *) malloc (sizeof(uv_timer_t));
  timer_u->data = data;
  
  int ret = uv_timer_init(lup_u, timer_u);
  if (ret <0){
    fprintf(stderr, "error init timer\n");
    exit(-1);
  }

  ret = uv_timer_start(timer_u,
                       func_ptr,
                       first_d,
                       thereafter_d
                       );

  if (ret <0){
    fprintf(stderr, "error init start\n");
    exit(-1);
  }

}

void util_run_loop()
{
  // head into event loop
  if ( u2_no == u2_Host.ops_u.bat ) {
    uv_run(u2L, UV_RUN_DEFAULT);
  }
}

